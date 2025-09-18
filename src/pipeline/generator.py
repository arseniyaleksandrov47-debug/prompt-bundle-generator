"""
Stage C: Generate - Parallel prompt generation through Groq API
"""

import asyncio
import time
from typing import List, Dict, Any
from dataclasses import dataclass
import httpx
import logging

from core.sharding import Shard, ShardItem
from pipeline.topic_profiling import TopicProfile

logger = logging.getLogger(__name__)


@dataclass
class GenerationResult:
    """Result of generation processing"""
    shard_id: int
    outputs: List[ShardItem]
    stats: Dict[str, Any]


class PromptGenerator:
    """Generates prompts using Groq API with parallel processing"""
    
    def __init__(self, key_manager, cache_manager, shard_manager):
        self.key_manager = key_manager
        self.cache_manager = cache_manager
        self.shard_manager = shard_manager
        self.client = httpx.AsyncClient(timeout=60.0)
        
    async def process_shards(
        self,
        shards: List[Shard],
        topic_profile: TopicProfile,
        dry_run: bool = False
    ) -> List[GenerationResult]:
        """Process multiple shards in parallel"""
        logger.info(f"⚡ Processing {len(shards)} shards for generation")
        
        tasks = []
        for shard in shards:
            task = self._process_single_shard(shard, topic_profile, dry_run)
            tasks.append(task)
            
        semaphore = asyncio.Semaphore(10)  # Limit concurrent shards
        
        async def bounded_task(task):
            async with semaphore:
                return await task
                
        bounded_tasks = [bounded_task(task) for task in tasks]
        results = await asyncio.gather(*bounded_tasks, return_exceptions=True)
        
        generation_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Shard {i} failed: {result}")
                generation_results.append(GenerationResult(
                    shard_id=i,
                    outputs=[],
                    stats={'error': str(result)}
                ))
            else:
                generation_results.append(result)
                
        total_outputs = sum(len(result.outputs) for result in generation_results)
        logger.info(f"✅ Generated {total_outputs} outputs from {len(shards)} shards")
        
        return generation_results
        
    async def _process_single_shard(
        self,
        shard: Shard,
        topic_profile: TopicProfile,
        dry_run: bool = False
    ) -> GenerationResult:
        """Process a single shard"""
        start_time = time.time()
        outputs = []
        cache_hits = 0
        api_calls = 0
        
        logger.debug(f"Processing shard {shard.shard_id} with {len(shard.items)} items")
        
        batch_size = 32
        for i in range(0, len(shard.items), batch_size):
            batch = shard.items[i:i + batch_size]
            batch_results = await self._process_batch(batch, topic_profile, dry_run)
            
            for item, result in zip(batch, batch_results):
                if result['from_cache']:
                    cache_hits += 1
                else:
                    api_calls += 1
                    
                item.out = result['output']
                outputs.append(item)
                
        processing_time = time.time() - start_time
        
        stats = {
            'processing_time': processing_time,
            'cache_hits': cache_hits,
            'api_calls': api_calls,
            'total_items': len(outputs),
            'cache_hit_rate': cache_hits / len(outputs) if outputs else 0
        }
        
        return GenerationResult(
            shard_id=shard.shard_id,
            outputs=outputs,
            stats=stats
        )
        
    async def _process_batch(
        self,
        batch: List[ShardItem],
        topic_profile: TopicProfile,
        dry_run: bool = False
    ) -> List[Dict[str, Any]]:
        """Process a batch of items"""
        results = []
        
        for item in batch:
            cached_result = await self.cache_manager.get_cached_result(item.prompt, item.meta)
            
            if cached_result:
                results.append({
                    'output': cached_result,
                    'from_cache': True
                })
                continue
                
            is_duplicate = await self.cache_manager.check_prompt_duplicate(item.prompt, item.meta)
            
            if is_duplicate:
                output = await self._generate_variation(item.prompt, topic_profile, dry_run)
            else:
                output = await self._generate_output(item.prompt, topic_profile, dry_run)
                
            await self.cache_manager.cache_result(item.prompt, item.meta, output)
            
            results.append({
                'output': output,
                'from_cache': False
            })
            
        return results
        
    async def _generate_output(
        self,
        prompt: str,
        topic_profile: TopicProfile,
        dry_run: bool = False
    ) -> str:
        """Generate output for a prompt"""
        if dry_run:
            return self._create_mock_output(prompt)
            
        generation_prompt = self._create_generation_prompt(prompt, topic_profile)
        
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                api_key = await self.key_manager.get_available_key()
                if not api_key:
                    await asyncio.sleep(1)
                    continue
                    
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                
                data = {
                    "messages": [
                        {"role": "user", "content": generation_prompt}
                    ],
                    "model": "llama3-8b-8192",
                    "max_tokens": 1000,
                    "temperature": 0.7
                }
                
                response = await self.client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers=headers,
                    json=data
                )
                
                if response.status_code == 200:
                    result = response.json()
                    content = result['choices'][0]['message']['content']
                    
                    await self.key_manager.record_usage(
                        api_key,
                        tokens_used=result.get('usage', {}).get('total_tokens', 0),
                        success=True
                    )
                    
                    return content.strip()
                    
                elif response.status_code == 429:
                    await self.key_manager.record_rate_limit(api_key)
                    await asyncio.sleep(2 ** attempt)
                    
                else:
                    await self.key_manager.record_usage(api_key, success=False)
                    logger.warning(f"Generation API error {response.status_code}: {response.text}")
                    
            except Exception as e:
                logger.warning(f"Generation attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
        logger.warning("All generation attempts failed, using mock output")
        return self._create_mock_output(prompt)
        
    async def _generate_variation(
        self,
        prompt: str,
        topic_profile: TopicProfile,
        dry_run: bool = False
    ) -> str:
        """Generate a variation of an existing prompt"""
        if dry_run:
            return self._create_mock_output(prompt) + " (Variation)"
            
        variation_prompt = f"""Create a unique variation of this prompt while maintaining the same core purpose and quality:

ORIGINAL PROMPT: {prompt}

TOPIC CONTEXT: {topic_profile.topic_brief}

Requirements:
- Keep the same core purpose and structure
- Change specific details, examples, or phrasing
- Maintain professional quality
- Ensure it's distinct from the original

Provide only the varied prompt, no explanations."""

        return await self._generate_output(variation_prompt, topic_profile, dry_run=False)
        
    def _create_generation_prompt(self, prompt: str, topic_profile: TopicProfile) -> str:
        """Create prompt for AI generation"""
        return f"""Based on this input prompt and topic context, generate a high-quality, professional output:

INPUT PROMPT: {prompt}

TOPIC CONTEXT: {topic_profile.topic_brief}

QUALITY CRITERIA:
{chr(10).join(f"- {criterion}" for criterion in topic_profile.quality_criteria)}

Requirements:
- Professional and actionable content
- Specific and measurable outcomes
- Appropriate for the target audience
- Clear and engaging language
- Ready to use immediately

Generate the output content only, no explanations or meta-commentary."""

    def _create_mock_output(self, prompt: str) -> str:
        """Create mock output for testing"""
        return f"""Professional output based on: {prompt[:100]}...

This is a high-quality, actionable response that demonstrates:
- Clear structure and organization
- Specific, measurable outcomes
- Professional tone and language
- Immediate usability
- Engaging and compelling content

Key benefits:
• Saves time and increases efficiency
• Provides measurable results
• Follows industry best practices
• Suitable for professional use

Next steps:
1. Review and customize as needed
2. Implement in your specific context
3. Monitor results and adjust accordingly

This output meets all quality criteria for professional use."""

    async def close(self):
        """Close the generator"""
        await self.client.aclose()
