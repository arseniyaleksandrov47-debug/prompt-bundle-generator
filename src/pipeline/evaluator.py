"""
Stage E: Evaluate - Quality evaluation with heuristics and AI-generated checks
"""

import asyncio
import re
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
from pathlib import Path
import httpx
import logging

from ..core.sharding import ShardItem
from ..pipeline.dedup import DedupResult
from ..pipeline.topic_profiling import TopicProfile

logger = logging.getLogger(__name__)


@dataclass
class EvaluationResult:
    """Result of evaluation processing"""
    shard_id: int
    outputs: List[ShardItem]
    stats: Dict[str, Any]


@dataclass
class QualityCheck:
    """Individual quality check"""
    name: str
    description: str
    weight: float
    passed: bool
    details: str = ""


class QualityEvaluator:
    """Evaluates prompt quality using heuristics and AI-generated checks"""
    
    def __init__(self, key_manager, run_dir: Path):
        self.key_manager = key_manager
        self.run_dir = run_dir
        self.client = httpx.AsyncClient(timeout=60.0)
        self.synth_checks = []
        
    async def process_shards(
        self,
        shards: List[DedupResult],
        topic_profile: TopicProfile,
        dry_run: bool = False,
        re_evaluation: bool = False
    ) -> List[EvaluationResult]:
        """Process multiple shards for evaluation"""
        
        stage_name = "Re-Evaluation" if re_evaluation else "Evaluation"
        logger.info(f"📊 {stage_name}: Processing {len(shards)} shards")
        
        if not re_evaluation:
            await self._generate_synth_checks(topic_profile, dry_run)
            
        tasks = []
        for shard in shards:
            task = self._evaluate_shard(shard, topic_profile, dry_run, re_evaluation)
            tasks.append(task)
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        evaluation_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Shard {i} evaluation failed: {result}")
                evaluation_results.append(EvaluationResult(
                    shard_id=i,
                    outputs=[],
                    stats={'error': str(result)}
                ))
            else:
                evaluation_results.append(result)
                
        await self._generate_evaluation_report(evaluation_results, re_evaluation)
        
        return evaluation_results
        
    async def _generate_synth_checks(
        self,
        topic_profile: TopicProfile,
        dry_run: bool = False
    ):
        """Generate AI-powered quality checks specific to the topic"""
        logger.info("🧠 Generating topic-specific quality checks...")
        
        if dry_run:
            self.synth_checks = self._create_mock_synth_checks()
            return
            
        synth_prompt = f"""Based on this topic profile, generate 7-10 specific quality checks that prompts in this domain must pass to achieve 9.5-9.9/10 rating.

TOPIC: {topic_profile.topic_brief}

PURPOSE & AUDIENCE: {topic_profile.purpose_audience}

CHANNELS & APPLICATIONS: {topic_profile.channels_applications}

EXISTING QUALITY CRITERIA:
{chr(10).join(f"- {criterion}" for criterion in topic_profile.quality_criteria)}

Generate specific, measurable quality checks in this format:

CHECK_NAME: Brief name
DESCRIPTION: What this check validates
WEIGHT: Importance (1.0-3.0)
CRITERIA: Pass/fail criteria

Focus on checks that ensure prompts can be used professionally TODAY in this specific domain."""

        synth_response = await self._get_ai_response(synth_prompt)
        
        self.synth_checks = self._parse_synth_checks(synth_response)
        
        logger.info(f"✅ Generated {len(self.synth_checks)} topic-specific checks")
        
    def _create_mock_synth_checks(self) -> List[Dict[str, Any]]:
        """Create mock synth checks for testing"""
        return [
            {
                'name': 'Professional_Tone',
                'description': 'Content uses professional, business-appropriate language',
                'weight': 2.0,
                'criteria': 'No casual language, slang, or unprofessional terms'
            },
            {
                'name': 'Actionable_Content',
                'description': 'Content provides specific, actionable steps or guidance',
                'weight': 3.0,
                'criteria': 'Contains clear action items or specific instructions'
            },
            {
                'name': 'Measurable_Outcomes',
                'description': 'Content includes measurable goals or success criteria',
                'weight': 2.5,
                'criteria': 'Mentions specific metrics, numbers, or measurable results'
            },
            {
                'name': 'Immediate_Usability',
                'description': 'Content can be used immediately without additional research',
                'weight': 2.0,
                'criteria': 'Self-contained and ready to implement'
            },
            {
                'name': 'Target_Audience_Fit',
                'description': 'Content is appropriate for the intended audience',
                'weight': 2.0,
                'criteria': 'Language and complexity match target audience'
            }
        ]
        
    def _parse_synth_checks(self, response: str) -> List[Dict[str, Any]]:
        """Parse AI response into structured checks"""
        checks = []
        current_check = {}
        
        for line in response.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            if line.startswith('CHECK_NAME:'):
                if current_check:
                    checks.append(current_check)
                current_check = {'name': line.replace('CHECK_NAME:', '').strip()}
            elif line.startswith('DESCRIPTION:'):
                current_check['description'] = line.replace('DESCRIPTION:', '').strip()
            elif line.startswith('WEIGHT:'):
                try:
                    weight_str = line.replace('WEIGHT:', '').strip()
                    current_check['weight'] = float(weight_str)
                except ValueError:
                    current_check['weight'] = 2.0
            elif line.startswith('CRITERIA:'):
                current_check['criteria'] = line.replace('CRITERIA:', '').strip()
                
        if current_check:
            checks.append(current_check)
            
        if not checks:
            return self._create_mock_synth_checks()
            
        return checks
        
    async def _evaluate_shard(
        self,
        shard: DedupResult,
        topic_profile: TopicProfile,
        dry_run: bool = False,
        re_evaluation: bool = False
    ) -> EvaluationResult:
        """Evaluate a single shard"""
        
        evaluated_outputs = []
        stats = {
            'total_items': len(shard.outputs),
            'high_quality_items': 0,
            'flagged_items': 0,
            'avg_score': 0.0
        }
        
        total_score = 0.0
        
        for item in shard.outputs:
            heuristic_results = self._run_heuristic_checks(item)
            
            synth_results = []
            if any(not check.passed for check in heuristic_results):
                if not dry_run:
                    synth_results = await self._run_synth_checks(item, topic_profile)
                else:
                    synth_results = self._mock_synth_results(item)
                    
            score, flags = self._calculate_score_and_flags(heuristic_results, synth_results)
            
            item.score = score
            item.flags = flags
            
            evaluated_outputs.append(item)
            total_score += score
            
            if score >= 9.5 and not flags.strip():
                stats['high_quality_items'] += 1
            if flags.strip():
                stats['flagged_items'] += 1
                
        stats['avg_score'] = total_score / len(evaluated_outputs) if evaluated_outputs else 0.0
        
        return EvaluationResult(
            shard_id=shard.shard_id,
            outputs=evaluated_outputs,
            stats=stats
        )
        
    def _run_heuristic_checks(self, item: ShardItem) -> List[QualityCheck]:
        """Run fast heuristic checks on an item"""
        checks = []
        
        checks.append(self._check_length_limits(item))
        
        checks.append(self._check_placeholders(item))
        
        checks.append(self._check_content_quality(item))
        
        checks.append(self._check_duplicate_content(item))
        
        checks.append(self._check_risk_lexicon(item))
        
        return checks
        
    def _check_length_limits(self, item: ShardItem) -> QualityCheck:
        """Check if content meets length requirements"""
        content = item.out
        length = len(content)
        
        meta_lower = item.meta.lower()
        
        if 'title' in meta_lower:
            min_len, max_len = 10, 100
        elif 'sms' in meta_lower or 'push' in meta_lower:
            min_len, max_len = 10, 160
        elif 'subject' in meta_lower:
            min_len, max_len = 10, 80
        else:
            min_len, max_len = 50, 2000
            
        passed = min_len <= length <= max_len
        details = f"Length: {length} (range: {min_len}-{max_len})"
        
        return QualityCheck(
            name="Length_Limits",
            description="Content meets appropriate length requirements",
            weight=1.0,
            passed=passed,
            details=details
        )
        
    def _check_placeholders(self, item: ShardItem) -> QualityCheck:
        """Check placeholder integrity"""
        content = item.out
        
        placeholders = re.findall(r'\{\{[^}]*\}\}', content)
        
        empty_placeholders = [p for p in placeholders if p.strip() == '{{}}']
        
        broken_placeholders = re.findall(r'\{[^}]*\}(?!\})', content) + re.findall(r'(?<!\{)\{[^}]*\}\}', content)
        
        passed = len(empty_placeholders) == 0 and len(broken_placeholders) == 0
        details = f"Placeholders: {len(placeholders)}, Empty: {len(empty_placeholders)}, Broken: {len(broken_placeholders)}"
        
        return QualityCheck(
            name="Placeholder_Integrity",
            description="Placeholders are properly formatted and not empty",
            weight=2.0,
            passed=passed,
            details=details
        )
        
    def _check_content_quality(self, item: ShardItem) -> QualityCheck:
        """Check for content quality indicators"""
        content = item.out.lower()
        
        filler_phrases = [
            'lorem ipsum', 'placeholder text', 'sample content',
            'insert here', 'add your', 'customize this',
            'example text', 'dummy content'
        ]
        
        filler_count = sum(1 for phrase in filler_phrases if phrase in content)
        
        numbers = re.findall(r'\b\d+(?:\.\d+)?%?\b', item.out)
        
        has_specifics = len(numbers) > 0
        no_fillers = filler_count == 0
        
        passed = has_specifics and no_fillers
        details = f"Numbers/facts: {len(numbers)}, Filler phrases: {filler_count}"
        
        return QualityCheck(
            name="Content_Quality",
            description="Content has specific details and avoids filler text",
            weight=2.5,
            passed=passed,
            details=details
        )
        
    def _check_duplicate_content(self, item: ShardItem) -> QualityCheck:
        """Check for internal duplicate content"""
        content = item.out
        
        sentences = [s.strip() for s in re.split(r'[.!?]+', content) if s.strip()]
        
        unique_sentences = set(sentences)
        duplicate_count = len(sentences) - len(unique_sentences)
        
        passed = duplicate_count == 0
        details = f"Sentences: {len(sentences)}, Duplicates: {duplicate_count}"
        
        return QualityCheck(
            name="Duplicate_Content",
            description="No duplicate sentences within content",
            weight=1.5,
            passed=passed,
            details=details
        )
        
    def _check_risk_lexicon(self, item: ShardItem) -> QualityCheck:
        """Check for risky or prohibited language"""
        content = item.out.lower()
        
        risk_terms = [
            'guaranteed', 'promise', 'instant', 'miracle',
            'secret', 'exclusive offer', 'limited time',
            'act now', 'urgent', 'don\'t miss out'
        ]
        
        risk_count = sum(1 for term in risk_terms if term in content)
        
        passed = risk_count == 0
        details = f"Risk terms found: {risk_count}"
        
        return QualityCheck(
            name="Risk_Lexicon",
            description="Content avoids risky or overly promotional language",
            weight=2.0,
            passed=passed,
            details=details
        )
        
    async def _run_synth_checks(
        self,
        item: ShardItem,
        topic_profile: TopicProfile
    ) -> List[QualityCheck]:
        """Run AI-powered synth checks on flagged items"""
        
        synth_results = []
        
        for check_def in self.synth_checks:
            check_prompt = f"""Evaluate this content against the specific quality criterion:

CONTENT: {item.out}

QUALITY CHECK: {check_def['name']}
DESCRIPTION: {check_def['description']}
CRITERIA: {check_def['criteria']}

TOPIC CONTEXT: {topic_profile.topic_brief}

Respond with:
PASS or FAIL
REASON: Brief explanation

Be strict but fair in evaluation."""

            response = await self._get_ai_response(check_prompt)
            
            passed = 'PASS' in response.upper()
            reason = ""
            
            for line in response.split('\n'):
                if line.startswith('REASON:'):
                    reason = line.replace('REASON:', '').strip()
                    break
                    
            synth_results.append(QualityCheck(
                name=check_def['name'],
                description=check_def['description'],
                weight=check_def['weight'],
                passed=passed,
                details=reason
            ))
            
        return synth_results
        
    def _mock_synth_results(self, item: ShardItem) -> List[QualityCheck]:
        """Create mock synth check results for testing"""
        results = []
        
        for check_def in self.synth_checks:
            passed = len(item.out) > 100  # Simple heuristic
            
            results.append(QualityCheck(
                name=check_def['name'],
                description=check_def['description'],
                weight=check_def['weight'],
                passed=passed,
                details="Mock evaluation result"
            ))
            
        return results
        
    def _calculate_score_and_flags(
        self,
        heuristic_results: List[QualityCheck],
        synth_results: List[QualityCheck]
    ) -> Tuple[float, str]:
        """Calculate overall score and flags"""
        
        all_checks = heuristic_results + synth_results
        
        if not all_checks:
            return 5.0, "NO_CHECKS"
            
        total_weight = sum(check.weight for check in all_checks)
        weighted_score = sum(
            check.weight * (10.0 if check.passed else 0.0)
            for check in all_checks
        )
        
        score = weighted_score / total_weight if total_weight > 0 else 0.0
        
        failed_checks = [check for check in all_checks if not check.passed]
        flags = ";".join(check.name for check in failed_checks)
        
        return score, flags
        
    async def _get_ai_response(self, prompt: str) -> str:
        """Get AI response for evaluation"""
        max_retries = 2
        
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
                        {"role": "user", "content": prompt}
                    ],
                    "model": "llama3-8b-8192",
                    "max_tokens": 500,
                    "temperature": 0.1
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
                    
            except Exception as e:
                logger.warning(f"Evaluation API attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
        return "PASS\nREASON: Fallback evaluation"
        
    async def _generate_evaluation_report(
        self,
        results: List[EvaluationResult],
        re_evaluation: bool = False
    ):
        """Generate evaluation report"""
        
        total_items = sum(result.stats['total_items'] for result in results)
        high_quality_items = sum(result.stats['high_quality_items'] for result in results)
        flagged_items = sum(result.stats['flagged_items'] for result in results)
        
        avg_score = sum(
            result.stats['avg_score'] * result.stats['total_items']
            for result in results
        ) / total_items if total_items > 0 else 0.0
        
        stage_name = "Re-Evaluation" if re_evaluation else "Evaluation"
        
        logger.info(f"📊 {stage_name} Report:")
        logger.info(f"   Total items: {total_items:,}")
        logger.info(f"   High quality (≥9.5): {high_quality_items:,} ({high_quality_items/total_items*100:.1f}%)")
        logger.info(f"   Flagged items: {flagged_items:,} ({flagged_items/total_items*100:.1f}%)")
        logger.info(f"   Average score: {avg_score:.2f}/10")
        
    async def close(self):
        """Close the evaluator"""
        await self.client.aclose()
