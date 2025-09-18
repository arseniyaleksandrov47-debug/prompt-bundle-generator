"""
Stage D: Deduplication - Remove duplicate outputs using n-grams and semantic hashing
"""

import asyncio
from typing import List, Dict, Any, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict
import logging

from core.sharding import Shard, ShardItem
from pipeline.generator import GenerationResult

logger = logging.getLogger(__name__)


@dataclass
class DedupResult:
    """Result of deduplication processing"""
    shard_id: int
    outputs: List[ShardItem]
    stats: Dict[str, Any]


class DeduplicatorProcessor:
    """Removes duplicate outputs using multiple techniques"""
    
    def __init__(self, cache_manager):
        self.cache_manager = cache_manager
        
    async def process_shards(
        self,
        generation_results: List[GenerationResult]
    ) -> List[DedupResult]:
        """Process multiple shards for deduplication"""
        logger.info(f"🔄 Deduplicating {len(generation_results)} shards")
        
        all_outputs = []
        shard_mapping = {}
        
        for result in generation_results:
            for item in result.outputs:
                all_outputs.append(item)
                shard_mapping[item.id] = result.shard_id
                
        logger.info(f"📊 Processing {len(all_outputs)} total outputs")
        
        pre_dedup_outputs = await self._pre_deduplication(all_outputs)
        logger.info(f"📉 After pre-dedup: {len(pre_dedup_outputs)} outputs")
        
        post_dedup_outputs = await self._post_deduplication(pre_dedup_outputs)
        logger.info(f"📉 After post-dedup: {len(post_dedup_outputs)} outputs")
        
        dedup_results = self._group_into_shards(post_dedup_outputs, generation_results)
        
        await self._generate_dedup_report(
            original_count=len(all_outputs),
            pre_dedup_count=len(pre_dedup_outputs),
            final_count=len(post_dedup_outputs)
        )
        
        return dedup_results
        
    async def _pre_deduplication(self, outputs: List[ShardItem]) -> List[ShardItem]:
        """Remove duplicates by prompt content"""
        seen_prompts = set()
        unique_outputs = []
        
        for item in outputs:
            normalized_prompt = self.cache_manager.normalize_prompt(item.prompt, item.meta)
            
            if normalized_prompt not in seen_prompts:
                seen_prompts.add(normalized_prompt)
                unique_outputs.append(item)
                
        return unique_outputs
        
    async def _post_deduplication(self, outputs: List[ShardItem]) -> List[ShardItem]:
        """Remove duplicates by output content using multiple techniques"""
        
        outputs = self._remove_exact_duplicates(outputs)
        
        outputs = await self._remove_ngram_duplicates(outputs)
        
        outputs = await self._remove_semantic_duplicates(outputs)
        
        return outputs
        
    def _remove_exact_duplicates(self, outputs: List[ShardItem]) -> List[ShardItem]:
        """Remove exact duplicate outputs"""
        seen_outputs = set()
        unique_outputs = []
        
        for item in outputs:
            normalized_output = item.out.strip().lower()
            
            if normalized_output not in seen_outputs:
                seen_outputs.add(normalized_output)
                unique_outputs.append(item)
                
        logger.debug(f"Removed {len(outputs) - len(unique_outputs)} exact duplicates")
        return unique_outputs
        
    async def _remove_ngram_duplicates(
        self,
        outputs: List[ShardItem],
        similarity_threshold: float = 0.8,
        ngram_size: int = 3
    ) -> List[ShardItem]:
        """Remove outputs with high n-gram similarity"""
        
        if len(outputs) <= 1:
            return outputs
            
        to_remove = set()
        
        for i in range(len(outputs)):
            if i in to_remove:
                continue
                
            for j in range(i + 1, len(outputs)):
                if j in to_remove:
                    continue
                    
                similarity = self.cache_manager.calculate_ngram_similarity(
                    outputs[i].out,
                    outputs[j].out,
                    ngram_size
                )
                
                if similarity >= similarity_threshold:
                    if len(outputs[i].out) >= len(outputs[j].out):
                        to_remove.add(j)
                    else:
                        to_remove.add(i)
                        break
                        
        unique_outputs = [
            outputs[i] for i in range(len(outputs))
            if i not in to_remove
        ]
        
        logger.debug(f"Removed {len(to_remove)} n-gram duplicates")
        return unique_outputs
        
    async def _remove_semantic_duplicates(
        self,
        outputs: List[ShardItem],
        similarity_threshold: float = 0.9
    ) -> List[ShardItem]:
        """Remove outputs with high semantic similarity"""
        
        if len(outputs) <= 1:
            return outputs
            
        semantic_groups = defaultdict(list)
        
        for item in outputs:
            semantic_hash = self.cache_manager.get_semantic_hash(item.out)
            semantic_groups[semantic_hash].append(item)
            
        unique_outputs = []
        removed_count = 0
        
        for group_items in semantic_groups.values():
            if len(group_items) == 1:
                unique_outputs.extend(group_items)
            else:
                best_item = max(group_items, key=lambda x: len(x.out))
                unique_outputs.append(best_item)
                removed_count += len(group_items) - 1
                
        logger.debug(f"Removed {removed_count} semantic duplicates")
        return unique_outputs
        
    def _group_into_shards(
        self,
        outputs: List[ShardItem],
        original_results: List[GenerationResult]
    ) -> List[DedupResult]:
        """Group deduplicated outputs back into shards"""
        
        shard_outputs = defaultdict(list)
        
        for item in outputs:
            original_shard_id = 0  # Default
            for result in original_results:
                for orig_item in result.outputs:
                    if orig_item.id == item.id:
                        original_shard_id = result.shard_id
                        break
                        
            shard_outputs[original_shard_id].append(item)
            
        dedup_results = []
        
        for result in original_results:
            shard_id = result.shard_id
            outputs_for_shard = shard_outputs[shard_id]
            
            original_count = len(result.outputs)
            final_count = len(outputs_for_shard)
            
            stats = {
                'original_count': original_count,
                'final_count': final_count,
                'removed_count': original_count - final_count,
                'dedup_rate': (original_count - final_count) / original_count if original_count > 0 else 0
            }
            
            dedup_results.append(DedupResult(
                shard_id=shard_id,
                outputs=outputs_for_shard,
                stats=stats
            ))
            
        return dedup_results
        
    async def _generate_dedup_report(
        self,
        original_count: int,
        pre_dedup_count: int,
        final_count: int
    ):
        """Generate deduplication report"""
        
        pre_removed = original_count - pre_dedup_count
        post_removed = pre_dedup_count - final_count
        total_removed = original_count - final_count
        
        report = f"""# Deduplication Report

- **Original outputs**: {original_count:,}
- **After pre-dedup**: {pre_dedup_count:,} (-{pre_removed:,})
- **Final outputs**: {final_count:,} (-{post_removed:,})
- **Total removed**: {total_removed:,} ({total_removed/original_count*100:.1f}%)


- Removed: {pre_removed:,} duplicate prompts
- Rate: {pre_removed/original_count*100:.1f}%

- Removed: {post_removed:,} duplicate outputs
- Rate: {post_removed/pre_dedup_count*100:.1f}%

1. **Exact matching**: Remove identical outputs
2. **N-gram similarity**: Remove outputs with >80% n-gram overlap
3. **Semantic hashing**: Group semantically similar outputs

- Maintained output diversity
- Preserved highest quality variants
- Reduced redundancy while keeping value
"""

        logger.info("📄 Deduplication report generated")
        logger.info(f"📊 Deduplication summary: {original_count} → {final_count} ({total_removed/original_count*100:.1f}% removed)")
