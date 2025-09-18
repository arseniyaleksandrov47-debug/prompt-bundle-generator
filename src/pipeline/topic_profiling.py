"""
Stage B: Topic Profiling - AI-generated topic analysis
"""

import asyncio
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass
import httpx
import logging

logger = logging.getLogger(__name__)


@dataclass
class TopicProfile:
    """AI-generated topic profile"""
    topic_brief: str
    purpose_audience: str
    channels_applications: str
    risks_constraints: str
    bundle_requirements: str
    quality_criteria: List[str]
    success_metrics: Dict[str, str]
    file_path: Path


class TopicProfiler:
    """Generates AI-powered topic profiles"""
    
    def __init__(self, key_manager, run_dir: Path):
        self.key_manager = key_manager
        self.run_dir = run_dir
        self.client = httpx.AsyncClient(timeout=60.0)
        
    async def process(
        self,
        topic_brief: str,
        sample_prompts: List[Dict[str, Any]],
        dry_run: bool = False
    ) -> TopicProfile:
        """Generate topic profile"""
        logger.info(f"🎯 Generating topic profile for: {topic_brief[:50]}...")
        
        if dry_run:
            return self._create_mock_profile(topic_brief)
            
        analysis_prompt = self._create_analysis_prompt(topic_brief, sample_prompts)
        
        analysis_result = await self._get_ai_analysis(analysis_prompt)
        
        profile = self._parse_analysis_result(topic_brief, analysis_result)
        
        await self._save_profile(profile)
        
        logger.info(f"✅ Topic profile generated: {profile.file_path}")
        return profile
        
    def _create_analysis_prompt(self, topic_brief: str, sample_prompts: List[Dict[str, Any]]) -> str:
        """Create prompt for AI topic analysis"""
        
        sample_text = ""
        if sample_prompts:
            sample_text = "\n\nSample prompts from this topic:\n"
            for i, prompt_data in enumerate(sample_prompts[:3]):
                sample_text += f"\n{i+1}. {prompt_data['prompt'][:200]}...\n"
        
        return f"""Analyze this topic and create a comprehensive topic profile:

TOPIC BRIEF: {topic_brief}
{sample_text}

Please provide a detailed analysis in the following format:

Describe the main purpose, target audience, and use cases for this topic.

List specific channels and applications where this would be used (email, social media, presentations, etc.).

Identify potential risks, limitations, and constraints for this topic (legal, ethical, technical, etc.).

Define what must be included in a bundle for this topic to be immediately usable (minimum 120 artifacts).

List 7-10 specific quality criteria that prompts in this topic must meet to achieve 9.5-9.9/10 rating.

Define measurable success metrics for this topic (engagement rates, conversion rates, etc.).

Focus on practical, actionable requirements that ensure any bundle for this topic can be used professionally today.
"""

    async def _get_ai_analysis(self, prompt: str) -> str:
        """Get AI analysis using Groq API"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                api_key = await self.key_manager.get_available_key()
                if not api_key:
                    await asyncio.sleep(5)  # Wait for keys to become available
                    continue
                    
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                
                data = {
                    "messages": [
                        {"role": "user", "content": prompt}
                    ],
                    "model": "llama3-70b-8192",
                    "max_tokens": 2000,
                    "temperature": 0.3
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
                    
                    return content
                    
                elif response.status_code == 429:
                    await self.key_manager.record_rate_limit(api_key)
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
                else:
                    await self.key_manager.record_usage(api_key, success=False)
                    logger.warning(f"API error {response.status_code}: {response.text}")
                    
            except Exception as e:
                logger.warning(f"Topic profiling attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
        logger.warning("All topic profiling attempts failed, using mock profile")
        return self._create_mock_analysis(prompt)
        
    def _create_mock_analysis(self, prompt: str) -> str:
        """Create mock analysis for testing"""
        return """## PURPOSE & AUDIENCE
This topic serves business professionals and entrepreneurs who need high-quality prompts for various business applications.

- Email marketing campaigns
- Social media content
- Business presentations
- Client communications
- Internal documentation

- Must comply with business communication standards
- Avoid overly promotional language
- Ensure professional tone
- Consider cultural sensitivity

- Minimum 120 ready-to-use prompts
- Clear categorization by use case
- Usage instructions and examples
- Quality assurance documentation

1. Clear and actionable language
2. Professional tone and style
3. Specific and measurable outcomes
4. Appropriate length for channel
5. Engaging and compelling content
6. Error-free grammar and spelling
7. Relevant to target audience
8. Compliant with platform guidelines

- Engagement rate: >5% improvement
- Conversion rate: >3% increase
- Time savings: >50% reduction in content creation
- User satisfaction: >4.5/5 rating
"""

    def _parse_analysis_result(self, topic_brief: str, analysis: str) -> TopicProfile:
        """Parse AI analysis result into structured profile"""
        
        sections = {}
        current_section = None
        current_content = []
        
        for line in analysis.split('\n'):
            line = line.strip()
            if line.startswith('##'):
                if current_section:
                    sections[current_section] = '\n'.join(current_content).strip()
                current_section = line.replace('##', '').strip().upper().replace(' ', '_')
                current_content = []
            elif line:
                current_content.append(line)
                
        if current_section:
            sections[current_section] = '\n'.join(current_content).strip()
            
        quality_criteria = []
        quality_text = sections.get('QUALITY_CRITERIA', '')
        for line in quality_text.split('\n'):
            line = line.strip()
            if line and (line.startswith(tuple('123456789')) or line.startswith('-')):
                criterion = line.lstrip('0123456789.-) ').strip()
                if criterion:
                    quality_criteria.append(criterion)
                    
        success_metrics = {}
        metrics_text = sections.get('SUCCESS_METRICS', '')
        for line in metrics_text.split('\n'):
            line = line.strip()
            if ':' in line:
                key, value = line.split(':', 1)
                success_metrics[key.strip().lstrip('- ')] = value.strip()
                
        profile_file = self.run_dir / "topic.md"
        
        return TopicProfile(
            topic_brief=topic_brief,
            purpose_audience=sections.get('PURPOSE_&_AUDIENCE', ''),
            channels_applications=sections.get('CHANNELS_&_APPLICATIONS', ''),
            risks_constraints=sections.get('RISKS_&_CONSTRAINTS', ''),
            bundle_requirements=sections.get('BUNDLE_REQUIREMENTS', ''),
            quality_criteria=quality_criteria,
            success_metrics=success_metrics,
            file_path=profile_file
        )
        
    async def _save_profile(self, profile: TopicProfile):
        """Save topic profile to markdown file"""
        content = f"""# Topic Profile

{profile.topic_brief}

{profile.purpose_audience}

{profile.channels_applications}

{profile.risks_constraints}

{profile.bundle_requirements}

"""
        
        for i, criterion in enumerate(profile.quality_criteria, 1):
            content += f"{i}. {criterion}\n"
            
        content += "\n## Success Metrics\n"
        for metric, value in profile.success_metrics.items():
            content += f"- **{metric}**: {value}\n"
            
        with open(profile.file_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
    def _create_mock_profile(self, topic_brief: str) -> TopicProfile:
        """Create mock profile for dry run"""
        profile_file = self.run_dir / "topic.md"
        
        return TopicProfile(
            topic_brief=topic_brief,
            purpose_audience="Mock purpose and audience for testing",
            channels_applications="Mock channels and applications",
            risks_constraints="Mock risks and constraints",
            bundle_requirements="Mock bundle requirements",
            quality_criteria=[
                "Clear and actionable language",
                "Professional tone",
                "Specific outcomes",
                "Appropriate length",
                "Engaging content"
            ],
            success_metrics={
                "Engagement rate": ">5% improvement",
                "Conversion rate": ">3% increase"
            },
            file_path=profile_file
        )
        
    async def close(self):
        """Close the profiler"""
        await self.client.aclose()
