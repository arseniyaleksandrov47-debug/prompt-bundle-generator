"""
Deterministic sharding for parallel processing
"""

import hashlib
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass
import csv
import logging

logger = logging.getLogger(__name__)


@dataclass
class ShardItem:
    """Single item in a shard"""
    id: str
    prompt: str
    meta: str
    out: str = ""
    score: float = 0.0
    flags: str = ""


@dataclass
class Shard:
    """Collection of items for parallel processing"""
    shard_id: int
    file_path: Path
    items: List[ShardItem]
    
    def __len__(self):
        return len(self.items)


class ShardManager:
    """Manages deterministic sharding for parallel processing"""
    
    def __init__(self, temp_dir: Path, shard_size: int = 1000):
        self.temp_dir = temp_dir
        self.shard_size = shard_size
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
    def get_shard_id(self, item_id: str, num_shards: int) -> int:
        """Get deterministic shard ID for an item"""
        hash_value = int(hashlib.md5(item_id.encode()).hexdigest(), 16)
        return hash_value % num_shards
        
    def create_shards(self, prompts: List[Dict[str, Any]]) -> List[Shard]:
        """Create shards from input prompts"""
        num_shards = max(1, (len(prompts) + self.shard_size - 1) // self.shard_size)
        
        shards = {}
        for i in range(num_shards):
            shard_file = self.temp_dir / f"shard_{i:04d}.tsv"
            shards[i] = Shard(
                shard_id=i,
                file_path=shard_file,
                items=[]
            )
        
        for prompt_data in prompts:
            item_id = prompt_data.get('id', str(hash(prompt_data.get('prompt', ''))))
            shard_id = self.get_shard_id(item_id, num_shards)
            
            item = ShardItem(
                id=item_id,
                prompt=prompt_data.get('prompt', ''),
                meta=prompt_data.get('meta', ''),
                out=prompt_data.get('out', ''),
                score=prompt_data.get('score', 0.0),
                flags=prompt_data.get('flags', '')
            )
            
            shards[shard_id].items.append(item)
        
        result_shards = []
        for shard in shards.values():
            if shard.items:  # Only include non-empty shards
                self._write_shard_to_file(shard)
                result_shards.append(shard)
        
        logger.info(f"📊 Created {len(result_shards)} shards from {len(prompts)} items")
        return result_shards
        
    def _write_shard_to_file(self, shard: Shard):
        """Write shard data to TSV file"""
        with open(shard.file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            
            writer.writerow(['id', 'prompt', 'meta', 'out', 'score', 'flags'])
            
            for item in shard.items:
                writer.writerow([
                    item.id,
                    self._escape_tsv_field(item.prompt),
                    self._escape_tsv_field(item.meta),
                    self._escape_tsv_field(item.out),
                    item.score,
                    self._escape_tsv_field(item.flags)
                ])
                
    def _escape_tsv_field(self, field: str) -> str:
        """Escape tabs and newlines in TSV field"""
        if not field:
            return ""
        return field.replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
        
    def _unescape_tsv_field(self, field: str) -> str:
        """Unescape tabs and newlines in TSV field"""
        if not field:
            return ""
        return field.replace('\\t', '\t').replace('\\n', '\n').replace('\\r', '\r')
        
    def load_shard_from_file(self, file_path: Path) -> Shard:
        """Load shard from TSV file"""
        items = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            
            next(reader, None)
            
            for row in reader:
                if len(row) >= 6:
                    item = ShardItem(
                        id=row[0],
                        prompt=self._unescape_tsv_field(row[1]),
                        meta=self._unescape_tsv_field(row[2]),
                        out=self._unescape_tsv_field(row[3]),
                        score=float(row[4]) if row[4] else 0.0,
                        flags=self._unescape_tsv_field(row[5])
                    )
                    items.append(item)
        
        shard_id = 0
        if file_path.name.startswith('shard_'):
            try:
                shard_id = int(file_path.name.split('_')[1].split('.')[0])
            except (IndexError, ValueError):
                pass
        
        return Shard(
            shard_id=shard_id,
            file_path=file_path,
            items=items
        )
        
    def merge_shards(self, shards: List[Shard], output_file: Path):
        """Merge multiple shards into a single TSV file"""
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            
            writer.writerow(['id', 'prompt', 'meta', 'out', 'score', 'flags'])
            
            for shard in shards:
                for item in shard.items:
                    writer.writerow([
                        item.id,
                        self._escape_tsv_field(item.prompt),
                        self._escape_tsv_field(item.meta),
                        self._escape_tsv_field(item.out),
                        item.score,
                        self._escape_tsv_field(item.flags)
                    ])
        
        logger.info(f"📄 Merged {len(shards)} shards into {output_file}")
        
    def get_shard_stats(self, shards: List[Shard]) -> Dict[str, Any]:
        """Get statistics for shards"""
        total_items = sum(len(shard) for shard in shards)
        avg_items_per_shard = total_items / len(shards) if shards else 0
        
        high_quality_items = 0
        flagged_items = 0
        
        for shard in shards:
            for item in shard.items:
                if item.score >= 9.5 and not (item.flags and item.flags.strip()):
                    high_quality_items += 1
                if item.flags and item.flags.strip():
                    flagged_items += 1
        
        return {
            'num_shards': len(shards),
            'total_items': total_items,
            'avg_items_per_shard': avg_items_per_shard,
            'high_quality_items': high_quality_items,
            'flagged_items': flagged_items,
            'quality_rate': high_quality_items / total_items if total_items > 0 else 0
        }
