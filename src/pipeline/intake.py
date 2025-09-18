"""
Stage A: Intake - Load and validate input data
"""

import csv
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class IntakeResult:
    """Result of intake processing"""
    prompts: List[Dict[str, Any]]
    topic_brief: str
    constraints: Dict[str, Any]
    metadata: Dict[str, Any]


class IntakeProcessor:
    """Processes input data and validates constraints"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    async def process(
        self,
        topic_brief: str,
        input_file: str,
        target_count: int,
        language: str
    ) -> IntakeResult:
        """Process intake stage"""
        logger.info(f"📥 Processing intake: {input_file}")
        
        prompts = await self._load_prompts(input_file)
        
        constraints = {
            'language': language,
            'target_count': target_count,
            'min_length': self.config.get('min_prompt_length', 50),
            'max_length': self.config.get('max_prompt_length', 2000),
            'forbidden_patterns': self.config.get('forbidden_patterns', [])
        }
        
        valid_prompts = []
        for prompt_data in prompts:
            if self._validate_prompt(prompt_data, constraints):
                valid_prompts.append(prompt_data)
                
        if len(valid_prompts) > target_count:
            valid_prompts = valid_prompts[:target_count]
            
        logger.info(f"✅ Loaded {len(valid_prompts)} valid prompts")
        
        return IntakeResult(
            prompts=valid_prompts,
            topic_brief=topic_brief,
            constraints=constraints,
            metadata={
                'input_file': input_file,
                'original_count': len(prompts),
                'valid_count': len(valid_prompts),
                'filtered_count': len(prompts) - len(valid_prompts)
            }
        )
        
    async def _load_prompts(self, input_file: str) -> List[Dict[str, Any]]:
        """Load prompts from TSV file"""
        prompts = []
        input_path = Path(input_file)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
            
        with open(input_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            f.seek(0)
            
            if '\t' in first_line:
                reader = csv.DictReader(f, delimiter='\t')
                for i, row in enumerate(reader):
                    prompt_data = {
                        'id': row.get('id', f"prompt_{i:06d}"),
                        'prompt': row.get('prompt', ''),
                        'meta': row.get('meta', ''),
                        'out': row.get('out', ''),
                        'score': float(row.get('score', 0.0)) if row.get('score') else 0.0,
                        'flags': row.get('flags', '')
                    }
                    prompts.append(prompt_data)
            else:
                for i, line in enumerate(f):
                    line = line.strip()
                    if line:
                        prompt_data = {
                            'id': f"prompt_{i:06d}",
                            'prompt': line,
                            'meta': '',
                            'out': '',
                            'score': 0.0,
                            'flags': ''
                        }
                        prompts.append(prompt_data)
                        
        return prompts
        
    def _validate_prompt(self, prompt_data: Dict[str, Any], constraints: Dict[str, Any]) -> bool:
        """Validate a single prompt against constraints"""
        prompt = prompt_data.get('prompt', '')
        
        if len(prompt) < constraints['min_length']:
            return False
        if len(prompt) > constraints['max_length']:
            return False
            
        prompt_lower = prompt.lower()
        for pattern in constraints['forbidden_patterns']:
            if pattern.lower() in prompt_lower:
                return False
                
        return True
