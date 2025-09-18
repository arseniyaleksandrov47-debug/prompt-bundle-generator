#!/usr/bin/env python3
"""
Main orchestrator for the Prompt Bundle Generator pipeline
"""

import asyncio
import sys
import time
from pathlib import Path
from typing import Optional
import click
import yaml

from .pipeline.intake import IntakeProcessor
from .pipeline.topic_profiling import TopicProfiler
from .pipeline.generator import PromptGenerator
from .pipeline.dedup import DeduplicatorProcessor
from .pipeline.evaluator import QualityEvaluator
from .pipeline.patcher import PromptPatcher
from .pipeline.packager import BundlePackager
from .core.key_manager import GroqKeyManager
from .core.cache import CacheManager
from .core.sharding import ShardManager
from .utils.logger import setup_logger
from .utils.metrics import MetricsCollector


@click.command()
@click.option('--topic', required=True, help='Topic brief (free text)')
@click.option('--input', 'input_file', required=True, type=click.Path(exists=True), help='Input TSV file with prompts')
@click.option('--output', 'output_dir', default='./runs', help='Output directory for runs')
@click.option('--config', 'config_file', default='./profiles/config.yaml', help='Configuration file')
@click.option('--keys', 'keys_file', default='./profiles/keys.tsv', help='Groq API keys file')
@click.option('--golden', 'golden_dir', default='./golden', help='Golden test cases directory')
@click.option('--cache-dir', default='./cache', help='Cache directory')
@click.option('--temp-dir', default='./tmp', help='Temporary files directory')
@click.option('--target-count', default=1000, type=int, help='Target number of prompts')
@click.option('--batch-size', default=64, type=int, help='Batch size for processing')
@click.option('--language', default='en', help='Output language')
@click.option('--storefront', default='etsy', help='Target storefront (etsy/gumroad/kofi)')
@click.option('--dry-run', is_flag=True, help='Dry run without API calls')
async def main(
    topic: str,
    input_file: str,
    output_dir: str,
    config_file: str,
    keys_file: str,
    golden_dir: str,
    cache_dir: str,
    temp_dir: str,
    target_count: int,
    batch_size: int,
    language: str,
    storefront: str,
    dry_run: bool
):
    """Main pipeline orchestrator"""
    
    logger = setup_logger()
    logger.info("🚀 Starting Prompt Bundle Generator Pipeline")
    
    timestamp = int(time.time())
    run_hash = f"{hash(topic + input_file) % 10000:04d}"
    run_dir = Path(output_dir) / f"{timestamp}_{run_hash}"
    run_dir.mkdir(parents=True, exist_ok=True)
    
    metrics = MetricsCollector(run_dir / "metrics.tsv")
    
    try:
        config = {}
        if Path(config_file).exists():
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f) or {}
        
        temp_path = Path(temp_dir)
        if temp_path.exists():
            import shutil
            shutil.rmtree(temp_path)
        temp_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("🔧 Initializing core components...")
        
        key_manager = GroqKeyManager(keys_file)
        await key_manager.initialize()
        
        cache_manager = CacheManager(cache_dir)
        await cache_manager.initialize()
        
        shard_manager = ShardManager(temp_path, batch_size)
        
        stages = {
            'intake': IntakeProcessor(config),
            'topic_profiling': TopicProfiler(key_manager, run_dir),
            'generator': PromptGenerator(key_manager, cache_manager, shard_manager),
            'dedup': DeduplicatorProcessor(cache_manager),
            'evaluator': QualityEvaluator(key_manager, run_dir),
            'patcher': PromptPatcher(key_manager),
            'packager': BundlePackager(run_dir, storefront, language)
        }
        
        logger.info("📥 Stage A: Intake")
        metrics.start_stage("intake")
        
        intake_result = await stages['intake'].process(
            topic_brief=topic,
            input_file=input_file,
            target_count=target_count,
            language=language
        )
        
        metrics.finish_stage("intake", count=len(intake_result.prompts))
        logger.info(f"✅ Intake complete: {len(intake_result.prompts)} prompts loaded")
        
        logger.info("🎯 Stage B: Topic Profiling")
        metrics.start_stage("topic_profiling")
        
        topic_profile = await stages['topic_profiling'].process(
            topic_brief=topic,
            sample_prompts=intake_result.prompts[:10],
            dry_run=dry_run
        )
        
        metrics.finish_stage("topic_profiling", count=1)
        logger.info("✅ Topic profiling complete")
        
        logger.info("⚡ Stage C: Generate")
        metrics.start_stage("generation")
        
        shards = shard_manager.create_shards(intake_result.prompts)
        logger.info(f"📊 Created {len(shards)} shards")
        
        generation_results = await stages['generator'].process_shards(
            shards=shards,
            topic_profile=topic_profile,
            dry_run=dry_run
        )
        
        total_generated = sum(len(result.outputs) for result in generation_results)
        metrics.finish_stage("generation", count=total_generated)
        logger.info(f"✅ Generation complete: {total_generated} outputs")
        
        logger.info("🔄 Stage D: Deduplication")
        metrics.start_stage("dedup")
        
        dedup_results = await stages['dedup'].process_shards(generation_results)
        
        total_after_dedup = sum(len(result.outputs) for result in dedup_results)
        metrics.finish_stage("dedup", count=total_after_dedup)
        logger.info(f"✅ Deduplication complete: {total_after_dedup} unique outputs")
        
        logger.info("📊 Stage E: Evaluate")
        metrics.start_stage("evaluation")
        
        eval_results = await stages['evaluator'].process_shards(
            shards=dedup_results,
            topic_profile=topic_profile,
            dry_run=dry_run
        )
        
        metrics.finish_stage("evaluation", count=total_after_dedup)
        logger.info("✅ Evaluation complete")
        
        logger.info("🔧 Stage F: Patch")
        
        patched_results = eval_results
        for iteration in range(2):
            metrics.start_stage(f"patch_iter_{iteration + 1}")
            
            items_to_patch = []
            for shard in patched_results:
                for item in shard.outputs:
                    if item.flags and item.flags.strip():
                        items_to_patch.append(item)
            
            if not items_to_patch:
                logger.info(f"✅ No items need patching in iteration {iteration + 1}")
                break
                
            logger.info(f"🔧 Patching {len(items_to_patch)} items in iteration {iteration + 1}")
            
            patch_results = await stages['patcher'].process_items(
                items=items_to_patch,
                dry_run=dry_run
            )
            
            for shard in patched_results:
                for i, item in enumerate(shard.outputs):
                    for patched_item in patch_results:
                        if item.id == patched_item.id:
                            shard.outputs[i] = patched_item
                            break
            
            metrics.finish_stage(f"patch_iter_{iteration + 1}", count=len(items_to_patch))
            
            logger.info(f"📊 Stage G: Re-Evaluate (iteration {iteration + 1})")
            metrics.start_stage(f"re_eval_iter_{iteration + 1}")
            
            patched_results = await stages['evaluator'].process_shards(
                shards=patched_results,
                topic_profile=topic_profile,
                dry_run=dry_run,
                re_evaluation=True
            )
            
            metrics.finish_stage(f"re_eval_iter_{iteration + 1}", count=total_after_dedup)
        
        logger.info("📦 Stage H: Package")
        metrics.start_stage("packaging")
        
        bundle_path = await stages['packager'].create_bundle(
            shards=patched_results,
            topic_profile=topic_profile,
            metrics=metrics,
            title=f"Professional {topic.title()} Prompts Collection"
        )
        
        metrics.finish_stage("packaging", count=1)
        
        quality_stats = calculate_quality_stats(patched_results)
        
        logger.info("🎉 Pipeline Complete!")
        logger.info(f"📁 Bundle created: {bundle_path}")
        logger.info(f"📊 Quality stats: {quality_stats['high_quality']}/{quality_stats['total']} items ≥9.5 ({quality_stats['success_rate']:.1%})")
        logger.info(f"⏱️  Total time: {metrics.get_total_time():.1f}s")
        
        if quality_stats['success_rate'] >= 0.98:
            logger.info("✅ Release conditions met!")
        else:
            logger.warning(f"⚠️  Release conditions not met: {quality_stats['success_rate']:.1%} < 98%")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        if temp_path.exists():
            import shutil
            shutil.rmtree(temp_path)


def calculate_quality_stats(shards):
    """Calculate quality statistics"""
    total = 0
    high_quality = 0
    
    for shard in shards:
        for item in shard.outputs:
            total += 1
            if item.score >= 9.5 and not (item.flags and item.flags.strip()):
                high_quality += 1
    
    return {
        'total': total,
        'high_quality': high_quality,
        'success_rate': high_quality / total if total > 0 else 0
    }


if __name__ == "__main__":
    asyncio.run(main())
