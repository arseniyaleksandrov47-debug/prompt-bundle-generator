#!/usr/bin/env python3
"""
Command-line interface for the Prompt Bundle Generator
"""

import asyncio
import sys
from pathlib import Path
import click

sys.path.insert(0, str(Path(__file__).parent))

from main import main as main_pipeline


@click.group()
def cli():
    """Prompt Bundle Generator - Professional prompt generation and packaging system"""
    pass


@cli.command()
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
def generate(
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
    """Generate a complete prompt bundle"""
    
    result = asyncio.run(main_pipeline(
        topic=topic,
        input_file=input_file,
        output_dir=output_dir,
        config_file=config_file,
        keys_file=keys_file,
        golden_dir=golden_dir,
        cache_dir=cache_dir,
        temp_dir=temp_dir,
        target_count=target_count,
        batch_size=batch_size,
        language=language,
        storefront=storefront,
        dry_run=dry_run
    ))
    
    sys.exit(result)


@cli.command()
@click.option('--keys-file', default='./profiles/keys.tsv', help='Groq API keys file')
def test_keys(keys_file: str):
    """Test Groq API keys for validity"""
    
    async def test_keys_async():
        from core.key_manager import GroqKeyManager
        
        try:
            key_manager = GroqKeyManager(keys_file)
            await key_manager.initialize()
            
            stats = key_manager.get_stats()
            print(f"✅ Key Manager Initialized")
            print(f"📊 Total keys: {stats['total_keys']}")
            print(f"💚 Healthy keys: {stats['healthy_keys']}")
            print(f"❌ Errors: {stats['total_errors']}")
            print(f"⏱️  Rate limits: {stats['total_rate_limits']}")
            
            if stats['healthy_keys'] > 0:
                print(f"🎉 Ready to process with {stats['healthy_keys']} working keys!")
                return 0
            else:
                print("❌ No healthy keys found. Please check your API keys.")
                return 1
                
        except Exception as e:
            print(f"❌ Key test failed: {e}")
            return 1
        finally:
            await key_manager.close()
    
    result = asyncio.run(test_keys_async())
    sys.exit(result)


@cli.command()
@click.option('--golden-dir', default='./golden', help='Golden test cases directory')
@click.option('--keys-file', default='./profiles/keys.tsv', help='Groq API keys file')
@click.option('--dry-run', is_flag=True, help='Dry run without API calls')
def test_golden(golden_dir: str, keys_file: str, dry_run: bool):
    """Run golden test cases for quality validation"""
    
    async def test_golden_async():
        from core.key_manager import GroqKeyManager
        from pipeline.evaluator import QualityEvaluator
        from pipeline.topic_profiling import TopicProfile
        from core.sharding import ShardItem
        import csv
        from pathlib import Path
        
        try:
            golden_path = Path(golden_dir)
            test_files = list(golden_path.glob("*.tsv"))
            
            if not test_files:
                print(f"❌ No golden test files found in {golden_dir}")
                return 1
                
            print(f"🧪 Running golden tests from {len(test_files)} files...")
            
            if not dry_run:
                key_manager = GroqKeyManager(keys_file)
                await key_manager.initialize()
            else:
                key_manager = None
                
            evaluator = QualityEvaluator(key_manager, Path("./tmp"))
            
            total_tests = 0
            passed_tests = 0
            
            for test_file in test_files:
                print(f"\n📋 Testing: {test_file.name}")
                
                with open(test_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f, delimiter='\t')
                    
                    for row in reader:
                        total_tests += 1
                        
                        item = ShardItem(
                            id=row['id'],
                            prompt=row['prompt'],
                            meta=row['meta'],
                            out=row['prompt'],  # Use prompt as output for testing
                            score=0.0,
                            flags=""
                        )
                        
                        topic_profile = TopicProfile(
                            topic_brief="Golden test case",
                            purpose_audience="Test audience",
                            channels_applications="Test applications",
                            risks_constraints="Test constraints",
                            bundle_requirements="Test requirements",
                            quality_criteria=["Professional tone", "Clear content"],
                            success_metrics={"test": "metric"},
                            file_path=Path("./tmp/test_topic.md")
                        )
                        
                        heuristic_results = evaluator._run_heuristic_checks(item)
                        score, flags = evaluator._calculate_score_and_flags(heuristic_results, [])
                        
                        expected_score = float(row['expected_score'])
                        expected_flags = row['expected_flags']
                        
                        score_ok = abs(score - expected_score) < 1.0  # Allow 1.0 point tolerance
                        flags_ok = (not expected_flags and not flags) or (expected_flags and flags)
                        
                        if score_ok and flags_ok:
                            passed_tests += 1
                            print(f"  ✅ {row['id']}: Score {score:.1f} (expected {expected_score})")
                        else:
                            print(f"  ❌ {row['id']}: Score {score:.1f} (expected {expected_score}), Flags: {flags}")
            
            print(f"\n🎯 Golden Test Results:")
            print(f"   Total tests: {total_tests}")
            print(f"   Passed: {passed_tests}")
            print(f"   Success rate: {passed_tests/total_tests*100:.1f}%")
            
            if passed_tests == total_tests:
                print("🎉 All golden tests passed!")
                return 0
            else:
                print("⚠️  Some golden tests failed.")
                return 1
                
        except Exception as e:
            print(f"❌ Golden test failed: {e}")
            import traceback
            traceback.print_exc()
            return 1
        finally:
            if key_manager:
                await key_manager.close()
    
    result = asyncio.run(test_golden_async())
    sys.exit(result)


@cli.command()
def version():
    """Show version information"""
    print("Prompt Bundle Generator v1.0")
    print("Professional prompt generation and packaging system")
    print("Architecture: TSV-based pipeline with Groq API integration")


if __name__ == "__main__":
    cli()
