"""
Groq API Key Manager with rate limiting and health monitoring
"""

import asyncio
import time
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import httpx
import logging

logger = logging.getLogger(__name__)


@dataclass
class KeyStats:
    """Statistics for a single API key"""
    key: str
    current_rps: float = 0.0
    current_tpm: float = 0.0
    error_count: int = 0
    rate_limit_count: int = 0
    last_used: float = 0.0
    is_healthy: bool = True
    requests_window: List[float] = field(default_factory=list)
    tokens_window: List[Tuple[float, int]] = field(default_factory=list)


class GroqKeyManager:
    """Manages pool of Groq API keys with rate limiting and health monitoring"""
    
    def __init__(self, keys_file: str, max_rps_per_key: int = 30, max_tpm_per_key: int = 6000):
        self.keys_file = Path(keys_file)
        self.max_rps_per_key = max_rps_per_key
        self.max_tpm_per_key = max_tpm_per_key
        self.keys: Dict[str, KeyStats] = {}
        self.client = httpx.AsyncClient(timeout=30.0)
        self._lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize the key manager"""
        await self._load_keys()
        await self._health_check_all_keys()
        
    async def _load_keys(self):
        """Load API keys from file"""
        if not self.keys_file.exists():
            raise FileNotFoundError(f"Keys file not found: {self.keys_file}")
            
        with open(self.keys_file, 'r') as f:
            lines = f.readlines()
            
        keys = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                keys.append(line)
                
        if not keys:
            raise ValueError("No valid API keys found")
            
        logger.info(f"🔑 Loaded {len(keys)} API keys")
        
        for key in keys:
            self.keys[key] = KeyStats(key=key)
            
    async def _health_check_all_keys(self):
        """Check health of all API keys"""
        logger.info("🔍 Checking health of API keys...")
        
        tasks = []
        for key in self.keys.keys():
            tasks.append(self._health_check_key(key))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        healthy_count = 0
        for key, result in zip(self.keys.keys(), results):
            if isinstance(result, Exception):
                logger.warning(f"❌ Key health check failed: {key[:20]}... - {result}")
                self.keys[key].is_healthy = False
            else:
                self.keys[key].is_healthy = result
                if result:
                    healthy_count += 1
                    
        logger.info(f"✅ Healthy keys: {healthy_count}/{len(self.keys)}")
        
        if healthy_count == 0:
            raise RuntimeError("No healthy API keys available")
            
    async def _health_check_key(self, key: str) -> bool:
        """Check if a single key is healthy"""
        try:
            headers = {
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "messages": [{"role": "user", "content": "test"}],
                "model": "llama3-8b-8192",
                "max_tokens": 1
            }
            
            response = await self.client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json=data
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.debug(f"Key health check failed: {e}")
            return False
            
    async def get_available_key(self) -> Optional[str]:
        """Get an available key that's not rate limited"""
        async with self._lock:
            current_time = time.time()
            
            for stats in self.keys.values():
                stats.requests_window = [
                    t for t in stats.requests_window 
                    if current_time - t < 60
                ]
                
                stats.tokens_window = [
                    (t, tokens) for t, tokens in stats.tokens_window
                    if current_time - t < 60
                ]
                
                stats.current_rps = len(stats.requests_window) / 60.0
                stats.current_tpm = sum(tokens for _, tokens in stats.tokens_window)
            
            available_keys = []
            for key, stats in self.keys.items():
                if (stats.is_healthy and 
                    stats.current_rps < self.max_rps_per_key and
                    stats.current_tpm < self.max_tpm_per_key):
                    available_keys.append((key, stats))
                    
            if not available_keys:
                return None
                
            available_keys.sort(key=lambda x: (x[1].current_rps, x[1].current_tpm))
            
            return available_keys[0][0]
            
    async def record_usage(self, key: str, tokens_used: int = 0, success: bool = True):
        """Record usage of a key"""
        async with self._lock:
            if key not in self.keys:
                return
                
            stats = self.keys[key]
            current_time = time.time()
            
            stats.requests_window.append(current_time)
            stats.last_used = current_time
            
            if tokens_used > 0:
                stats.tokens_window.append((current_time, tokens_used))
                
            if not success:
                stats.error_count += 1
                
                if stats.error_count > 5:
                    stats.is_healthy = False
                    logger.warning(f"⚠️  Key marked unhealthy due to errors: {key[:20]}...")
                    
    async def record_rate_limit(self, key: str):
        """Record a rate limit hit"""
        async with self._lock:
            if key not in self.keys:
                return
                
            stats = self.keys[key]
            stats.rate_limit_count += 1
            
            stats.is_healthy = False
            
            asyncio.create_task(self._delayed_health_check(key, 60))
            
    async def _delayed_health_check(self, key: str, delay: int):
        """Re-check key health after delay"""
        await asyncio.sleep(delay)
        is_healthy = await self._health_check_key(key)
        
        async with self._lock:
            if key in self.keys:
                self.keys[key].is_healthy = is_healthy
                if is_healthy:
                    logger.info(f"✅ Key recovered: {key[:20]}...")
                    
    def get_stats(self) -> Dict:
        """Get statistics for all keys"""
        total_keys = len(self.keys)
        healthy_keys = sum(1 for stats in self.keys.values() if stats.is_healthy)
        total_errors = sum(stats.error_count for stats in self.keys.values())
        total_rate_limits = sum(stats.rate_limit_count for stats in self.keys.values())
        
        return {
            'total_keys': total_keys,
            'healthy_keys': healthy_keys,
            'total_errors': total_errors,
            'total_rate_limits': total_rate_limits,
            'avg_rps': sum(stats.current_rps for stats in self.keys.values()) / total_keys if total_keys > 0 else 0,
            'avg_tpm': sum(stats.current_tpm for stats in self.keys.values()) / total_keys if total_keys > 0 else 0
        }
        
    async def close(self):
        """Close the key manager"""
        await self.client.aclose()
