"""
Content-addressed cache with Bloom filters for deduplication
"""

import hashlib
import pickle
import time
from pathlib import Path
from typing import Any, Optional, Set
import aiofiles
import logging
from bloom_filter2 import BloomFilter

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages content-addressed cache and Bloom filters"""
    
    def __init__(self, cache_dir: str, bloom_capacity: int = 100000, bloom_error_rate: float = 0.1):
        self.cache_dir = Path(cache_dir)
        self.bloom_capacity = bloom_capacity
        self.bloom_error_rate = bloom_error_rate
        
        self.prompt_bloom: Optional[BloomFilter] = None
        self.output_bloom: Optional[BloomFilter] = None
        
        self.cache_hits = 0
        self.cache_misses = 0
        self.bloom_hits = 0
        
    async def initialize(self):
        """Initialize cache manager"""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.prompt_bloom = BloomFilter(max_elements=self.bloom_capacity, error_rate=self.bloom_error_rate)
        self.output_bloom = BloomFilter(max_elements=self.bloom_capacity, error_rate=self.bloom_error_rate)
        
        await self._load_bloom_filters()
        
        logger.info(f"💾 Cache initialized: {self.cache_dir}")
        
    async def _load_bloom_filters(self):
        """Load Bloom filters from disk"""
        prompt_bloom_file = self.cache_dir / "prompt_bloom.pkl"
        output_bloom_file = self.cache_dir / "output_bloom.pkl"
        
        try:
            if prompt_bloom_file.exists():
                async with aiofiles.open(prompt_bloom_file, 'rb') as f:
                    data = await f.read()
                    self.prompt_bloom = pickle.loads(data)
                    
            if output_bloom_file.exists():
                async with aiofiles.open(output_bloom_file, 'rb') as f:
                    data = await f.read()
                    self.output_bloom = pickle.loads(data)
                    
            logger.info("✅ Bloom filters loaded from disk")
            
        except Exception as e:
            logger.warning(f"⚠️  Failed to load Bloom filters: {e}")
            self.prompt_bloom = BloomFilter(max_elements=self.bloom_capacity, error_rate=self.bloom_error_rate)
            self.output_bloom = BloomFilter(max_elements=self.bloom_capacity, error_rate=self.bloom_error_rate)
            
    async def _save_bloom_filters(self):
        """Save Bloom filters to disk"""
        try:
            prompt_bloom_file = self.cache_dir / "prompt_bloom.pkl"
            output_bloom_file = self.cache_dir / "output_bloom.pkl"
            
            async with aiofiles.open(prompt_bloom_file, 'wb') as f:
                data = pickle.dumps(self.prompt_bloom)
                await f.write(data)
                
            async with aiofiles.open(output_bloom_file, 'wb') as f:
                data = pickle.dumps(self.output_bloom)
                await f.write(data)
                
        except Exception as e:
            logger.warning(f"⚠️  Failed to save Bloom filters: {e}")
            
    def normalize_prompt(self, prompt: str, meta: str = "") -> str:
        """Normalize prompt for consistent hashing"""
        normalized = " ".join(prompt.strip().lower().split())
        if meta:
            normalized += f"|{meta.strip().lower()}"
        return normalized
        
    def get_cache_key(self, prompt: str, meta: str = "") -> str:
        """Generate cache key for prompt + meta"""
        normalized = self.normalize_prompt(prompt, meta)
        return hashlib.sha1(normalized.encode()).hexdigest()
        
    async def check_prompt_duplicate(self, prompt: str, meta: str = "") -> bool:
        """Check if prompt is likely a duplicate using Bloom filter"""
        normalized = self.normalize_prompt(prompt, meta)
        
        if normalized in self.prompt_bloom:
            self.bloom_hits += 1
            return True
            
        self.prompt_bloom.add(normalized)
        return False
        
    async def check_output_duplicate(self, output: str) -> bool:
        """Check if output is likely a duplicate using Bloom filter"""
        normalized = output.strip().lower()
        
        if normalized in self.output_bloom:
            self.bloom_hits += 1
            return True
            
        self.output_bloom.add(normalized)
        return False
        
    async def get_cached_result(self, prompt: str, meta: str = "") -> Optional[Any]:
        """Get cached result for prompt"""
        cache_key = self.get_cache_key(prompt, meta)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        try:
            if cache_file.exists():
                async with aiofiles.open(cache_file, 'rb') as f:
                    data = await f.read()
                    result = pickle.loads(data)
                    
                if time.time() - result.get('timestamp', 0) < 86400:
                    self.cache_hits += 1
                    return result.get('data')
                else:
                    cache_file.unlink()
                    
        except Exception as e:
            logger.debug(f"Cache read error: {e}")
            
        self.cache_misses += 1
        return None
        
    async def cache_result(self, prompt: str, meta: str, result: Any):
        """Cache result for prompt"""
        cache_key = self.get_cache_key(prompt, meta)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        try:
            cache_data = {
                'data': result,
                'timestamp': time.time()
            }
            
            async with aiofiles.open(cache_file, 'wb') as f:
                data = pickle.dumps(cache_data)
                await f.write(data)
                
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
            
    def calculate_ngram_similarity(self, text1: str, text2: str, n: int = 3) -> float:
        """Calculate n-gram similarity between two texts"""
        def get_ngrams(text: str, n: int) -> Set[str]:
            words = text.lower().split()
            return set(' '.join(words[i:i+n]) for i in range(len(words) - n + 1))
            
        ngrams1 = get_ngrams(text1, n)
        ngrams2 = get_ngrams(text2, n)
        
        if not ngrams1 and not ngrams2:
            return 1.0
        if not ngrams1 or not ngrams2:
            return 0.0
            
        intersection = len(ngrams1 & ngrams2)
        union = len(ngrams1 | ngrams2)
        
        return intersection / union if union > 0 else 0.0
        
    def get_semantic_hash(self, text: str) -> str:
        """Generate semantic hash for text (simplified)"""
        common_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should'}
        
        words = [w.lower() for w in text.split() if w.lower() not in common_words and len(w) > 2]
        
        key_words = sorted(set(words))[:20]  # Take top 20 unique words
        
        return hashlib.md5(' '.join(key_words).encode()).hexdigest()[:16]
        
    def get_stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = self.cache_hits / total_requests if total_requests > 0 else 0
        
        return {
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'hit_rate': hit_rate,
            'bloom_hits': self.bloom_hits,
            'total_requests': total_requests
        }
        
    async def cleanup_old_cache(self, max_age_hours: int = 24):
        """Clean up old cache entries"""
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        
        cleaned_count = 0
        for cache_file in self.cache_dir.glob("*.pkl"):
            if cache_file.name.startswith(('prompt_bloom', 'output_bloom')):
                continue
                
            try:
                if current_time - cache_file.stat().st_mtime > max_age_seconds:
                    cache_file.unlink()
                    cleaned_count += 1
            except Exception:
                pass
                
        if cleaned_count > 0:
            logger.info(f"🧹 Cleaned {cleaned_count} old cache entries")
            
        await self._save_bloom_filters()
