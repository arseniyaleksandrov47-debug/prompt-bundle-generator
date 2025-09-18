"""
Stage F: Patch - Apply targeted fixes to flagged items
"""

import asyncio
import re
from typing import List, Dict, Any
from dataclasses import dataclass
import httpx
import logging

from core.sharding import ShardItem

logger = logging.getLogger(__name__)


@dataclass
class PatchResult:
    """Result of patching an item"""
    item: ShardItem
    patches_applied: List[str]
    success: bool
    error: str = ""


class PromptPatcher:
    """Applies targeted fixes to flagged prompts"""
    
    def __init__(self, key_manager):
        self.key_manager = key_manager
        self.client = httpx.AsyncClient(timeout=60.0)
        
        self.patch_strategies = {
            'Length_Limits': self._patch_length,
            'Placeholder_Integrity': self._patch_placeholders,
            'Content_Quality': self._patch_content_quality,
            'Duplicate_Content': self._patch_duplicates,
            'Risk_Lexicon': self._patch_risk_language,
            'Professional_Tone': self._patch_tone,
            'Actionable_Content': self._patch_actionability,
            'Measurable_Outcomes': self._patch_measurability,
            'Immediate_Usability': self._patch_usability,
            'Target_Audience_Fit': self._patch_audience_fit
        }
        
    async def process_items(
        self,
        items: List[ShardItem],
        dry_run: bool = False
    ) -> List[ShardItem]:
        """Process multiple items for patching"""
        logger.info(f"🔧 Patching {len(items)} flagged items")
        
        semaphore = asyncio.Semaphore(5)  # Limit concurrent patches
        
        async def bounded_patch(item):
            async with semaphore:
                return await self._patch_item(item, dry_run)
                
        tasks = [bounded_patch(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        patched_items = []
        successful_patches = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Patching item {items[i].id} failed: {result}")
                patched_items.append(items[i])  # Return original item
            else:
                patched_items.append(result)
                if result.id != items[i].id or result.out != items[i].out:
                    successful_patches += 1
                    
        logger.info(f"✅ Successfully patched {successful_patches}/{len(items)} items")
        return patched_items
        
    async def _patch_item(
        self,
        item: ShardItem,
        dry_run: bool = False
    ) -> ShardItem:
        """Patch a single item based on its flags"""
        
        if not item.flags or not item.flags.strip():
            return item  # No flags, no patching needed
            
        flags = [flag.strip() for flag in item.flags.split(';') if flag.strip()]
        
        if not flags:
            return item
            
        logger.debug(f"Patching item {item.id} with flags: {flags}")
        
        patched_item = ShardItem(
            id=item.id,
            prompt=item.prompt,
            meta=item.meta,
            out=item.out,
            score=item.score,
            flags=item.flags
        )
        
        patches_applied = []
        
        for flag in flags:
            if flag in self.patch_strategies:
                try:
                    patch_result = await self.patch_strategies[flag](patched_item, dry_run)
                    if patch_result:
                        patched_item.out = patch_result
                        patches_applied.append(flag)
                        logger.debug(f"Applied patch for {flag}")
                except Exception as e:
                    logger.warning(f"Failed to apply patch for {flag}: {e}")
                    
        if patches_applied:
            remaining_flags = [flag for flag in flags if flag not in patches_applied]
            patched_item.flags = ';'.join(remaining_flags)
            
        return patched_item
        
    async def _patch_length(self, item: ShardItem, dry_run: bool = False) -> str:
        """Fix length issues"""
        content = item.out
        current_length = len(content)
        
        meta_lower = item.meta.lower()
        
        if 'title' in meta_lower:
            target_min, target_max = 10, 100
        elif 'sms' in meta_lower or 'push' in meta_lower:
            target_min, target_max = 10, 160
        elif 'subject' in meta_lower:
            target_min, target_max = 10, 80
        else:
            target_min, target_max = 50, 2000
            
        if current_length < target_min:
            return await self._expand_content(content, target_min, dry_run)
        elif current_length > target_max:
            return await self._compress_content(content, target_max, dry_run)
        else:
            return content
            
    async def _expand_content(self, content: str, target_length: int, dry_run: bool = False) -> str:
        """Expand content to meet minimum length"""
        if dry_run:
            return content + " [Expanded content to meet length requirements]"
            
        expand_prompt = f"""Expand this content to be at least {target_length} characters while maintaining quality and relevance:

ORIGINAL: {content}

Requirements:
- Maintain the original meaning and purpose
- Add relevant details, examples, or explanations
- Keep professional tone
- Reach at least {target_length} characters
- Don't add filler or repetitive content

Provide only the expanded content:"""

        return await self._get_ai_patch(expand_prompt, content)
        
    async def _compress_content(self, content: str, target_length: int, dry_run: bool = False) -> str:
        """Compress content to meet maximum length"""
        if dry_run:
            return content[:target_length] + "..."
            
        compress_prompt = f"""Compress this content to be at most {target_length} characters while preserving key information:

ORIGINAL: {content}

Requirements:
- Keep the most important information
- Maintain professional tone
- Stay under {target_length} characters
- Don't lose core meaning
- Remove redundancy and filler

Provide only the compressed content:"""

        return await self._get_ai_patch(compress_prompt, content)
        
    async def _patch_placeholders(self, item: ShardItem, dry_run: bool = False) -> str:
        """Fix placeholder issues"""
        content = item.out
        
        if dry_run:
            content = re.sub(r'\{\{\s*\}\}', '{{placeholder}}', content)
            content = re.sub(r'(?<!\{)\{([^}]*)\}(?!\})', r'{{\1}}', content)
            return content
            
        placeholder_prompt = f"""Fix the placeholder formatting in this content:

CONTENT: {content}

Issues to fix:
- Empty placeholders {{{{}}}} should have descriptive names
- Single braces {{text}} should be double braces {{{{text}}}}
- Broken placeholder formatting

Requirements:
- Use descriptive placeholder names
- Ensure all placeholders use {{{{name}}}} format
- Keep original content structure
- Don't change non-placeholder content

Provide only the corrected content:"""

        return await self._get_ai_patch(placeholder_prompt, content)
        
    async def _patch_content_quality(self, item: ShardItem, dry_run: bool = False) -> str:
        """Improve content quality"""
        if dry_run:
            return item.out + " [Enhanced with specific details and professional language]"
            
        quality_prompt = f"""Improve the quality of this content by adding specific details and removing filler:

CONTENT: {item.out}

Requirements:
- Replace vague language with specific details
- Add concrete examples or numbers where appropriate
- Remove filler phrases and generic content
- Maintain professional tone
- Keep the same core purpose and structure

Provide only the improved content:"""

        return await self._get_ai_patch(quality_prompt, item.out)
        
    async def _patch_duplicates(self, item: ShardItem, dry_run: bool = False) -> str:
        """Remove duplicate content"""
        content = item.out
        
        if dry_run:
            sentences = content.split('.')
            unique_sentences = []
            seen = set()
            
            for sentence in sentences:
                sentence = sentence.strip()
                if sentence and sentence not in seen:
                    unique_sentences.append(sentence)
                    seen.add(sentence)
                    
            return '. '.join(unique_sentences)
            
        dedup_prompt = f"""Remove duplicate sentences and redundant content from this text:

CONTENT: {content}

Requirements:
- Remove exact duplicate sentences
- Eliminate redundant information
- Maintain logical flow and coherence
- Keep the most informative version of similar content
- Preserve original meaning and structure

Provide only the deduplicated content:"""

        return await self._get_ai_patch(dedup_prompt, content)
        
    async def _patch_risk_language(self, item: ShardItem, dry_run: bool = False) -> str:
        """Replace risky language with safer alternatives"""
        content = item.out
        
        if dry_run:
            replacements = {
                'guaranteed': 'designed to help',
                'promise': 'aim to',
                'instant': 'quick',
                'miracle': 'effective',
                'secret': 'proven',
                'exclusive offer': 'special opportunity',
                'limited time': 'available now',
                'act now': 'get started',
                'urgent': 'important',
                'don\'t miss out': 'take advantage'
            }
            
            for risky, safe in replacements.items():
                content = re.sub(risky, safe, content, flags=re.IGNORECASE)
                
            return content
            
        risk_prompt = f"""Replace risky or overly promotional language with professional alternatives:

CONTENT: {content}

Requirements:
- Replace guarantees with realistic expectations
- Change promotional language to professional tone
- Remove urgency tactics
- Maintain persuasive but ethical language
- Keep the same core message

Provide only the revised content:"""

        return await self._get_ai_patch(risk_prompt, content)
        
    async def _patch_tone(self, item: ShardItem, dry_run: bool = False) -> str:
        """Improve professional tone"""
        if dry_run:
            return item.out.replace('!', '.').replace('awesome', 'excellent').replace('amazing', 'effective')
            
        tone_prompt = f"""Improve the professional tone of this content:

CONTENT: {item.out}

Requirements:
- Use professional, business-appropriate language
- Remove casual expressions and slang
- Maintain engaging but formal tone
- Keep the same meaning and structure
- Ensure appropriate for business context

Provide only the professionally toned content:"""

        return await self._get_ai_patch(tone_prompt, item.out)
        
    async def _patch_actionability(self, item: ShardItem, dry_run: bool = False) -> str:
        """Make content more actionable"""
        if dry_run:
            return item.out + "\n\nNext steps:\n1. Review the requirements\n2. Implement the solution\n3. Monitor results"
            
        action_prompt = f"""Make this content more actionable by adding specific steps or guidance:

CONTENT: {item.out}

Requirements:
- Add clear, specific action items
- Include step-by-step guidance where appropriate
- Make instructions concrete and implementable
- Maintain original purpose and tone
- Ensure immediate usability

Provide only the enhanced actionable content:"""

        return await self._get_ai_patch(action_prompt, item.out)
        
    async def _patch_measurability(self, item: ShardItem, dry_run: bool = False) -> str:
        """Add measurable outcomes"""
        if dry_run:
            return item.out + " Target: 25% improvement in efficiency within 30 days."
            
        metrics_prompt = f"""Add measurable outcomes and success criteria to this content:

CONTENT: {item.out}

Requirements:
- Include specific metrics or KPIs
- Add measurable success criteria
- Provide concrete numbers where appropriate
- Maintain professional tone
- Keep original structure and purpose

Provide only the content with added measurability:"""

        return await self._get_ai_patch(metrics_prompt, item.out)
        
    async def _patch_usability(self, item: ShardItem, dry_run: bool = False) -> str:
        """Improve immediate usability"""
        if dry_run:
            return item.out + "\n\nReady to use: This content can be implemented immediately without additional research."
            
        usability_prompt = f"""Make this content immediately usable by adding necessary context and details:

CONTENT: {item.out}

Requirements:
- Ensure content is self-contained
- Add any missing context or prerequisites
- Include necessary details for immediate implementation
- Remove dependencies on external research
- Maintain professional quality

Provide only the enhanced usable content:"""

        return await self._get_ai_patch(usability_prompt, item.out)
        
    async def _patch_audience_fit(self, item: ShardItem, dry_run: bool = False) -> str:
        """Improve audience targeting"""
        if dry_run:
            return item.out.replace('you', 'business professionals').replace('your', 'their')
            
        audience_prompt = f"""Adjust this content to better fit the target audience:

CONTENT: {item.out}
META: {item.meta}

Requirements:
- Adjust language complexity for target audience
- Use appropriate terminology and examples
- Ensure tone matches audience expectations
- Maintain core message and value
- Consider audience's context and needs

Provide only the audience-adjusted content:"""

        return await self._get_ai_patch(audience_prompt, item.out)
        
    async def _get_ai_patch(self, prompt: str, fallback_content: str) -> str:
        """Get AI-generated patch"""
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
                    "max_tokens": 800,
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
                    
                    return content.strip()
                    
                elif response.status_code == 429:
                    await self.key_manager.record_rate_limit(api_key)
                    await asyncio.sleep(2 ** attempt)
                    
                else:
                    await self.key_manager.record_usage(api_key, success=False)
                    
            except Exception as e:
                logger.warning(f"Patch API attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
        logger.warning("Patch attempts failed, returning original content")
        return fallback_content
        
    async def close(self):
        """Close the patcher"""
        await self.client.aclose()
