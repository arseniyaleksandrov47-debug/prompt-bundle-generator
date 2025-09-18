#!/usr/bin/env python3
"""
Simple project runner - just edit profiles/project_config.yaml and run this script
"""

import asyncio
import sys
from pathlib import Path
import yaml

sys.path.insert(0, str(Path(__file__).parent / "src"))

from main import run_pipeline


def load_project_config():
    """Load project configuration from YAML file"""
    config_path = Path("profiles/project_config.yaml")
    
    if not config_path.exists():
        print("❌ Configuration file not found: profiles/project_config.yaml")
        print("Please create this file or use the CLI directly.")
        sys.exit(1)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config


async def main():
    """Main entry point"""
    print("🚀 Prompt Bundle Generator - Simple Runner")
    print("📝 Loading configuration from profiles/project_config.yaml...")
    
    try:
        config = load_project_config()
        
        project = config['project']
        input_config = config['input']
        processing = config['processing']
        output_config = config['output']
        advanced = config['advanced']
        
        print(f"🎯 Topic: {project['topic']}")
        print(f"📊 Target count: {project['target_count']}")
        print(f"📦 Batch size: {processing['batch_size']}")
        print(f"🌐 Language: {project['language']}")
        print(f"🏪 Storefront: {project['storefront']}")
        print(f"🧪 Dry run: {processing['dry_run']}")
        print()
        
        result = await run_pipeline(
            topic=project['topic'],
            input_file=input_config['file'],
            output_dir=output_config['directory'],
            config_file=advanced['config_file'],
            keys_file=advanced['keys_file'],
            golden_dir=advanced['golden_dir'],
            cache_dir=advanced['cache_dir'],
            temp_dir=advanced['temp_dir'],
            target_count=project['target_count'],
            batch_size=processing['batch_size'],
            language=project['language'],
            storefront=project['storefront'],
            dry_run=processing['dry_run']
        )
        
        if result == 0:
            print("\n🎉 Project completed successfully!")
            print("📁 Check the 'runs/' directory for your generated bundle.")
        else:
            print("\n❌ Project failed. Check the logs above for details.")
        
        sys.exit(result)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
