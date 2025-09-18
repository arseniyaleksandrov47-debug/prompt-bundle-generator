"""
Metrics collection and reporting for the pipeline
"""

import time
import csv
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class StageMetrics:
    """Metrics for a single pipeline stage"""
    name: str
    start_time: float
    end_time: Optional[float] = None
    count: int = 0
    success: bool = True
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> float:
        """Get stage duration in seconds"""
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time
    
    @property
    def throughput(self) -> float:
        """Get throughput (items per second)"""
        duration = self.duration
        if duration > 0 and self.count > 0:
            return self.count / duration
        return 0.0


class MetricsCollector:
    """Collects and reports pipeline metrics"""
    
    def __init__(self, metrics_file: Optional[Path] = None):
        self.metrics_file = metrics_file
        self.stages: Dict[str, StageMetrics] = {}
        self.pipeline_start_time = time.time()
        
    def start_stage(self, stage_name: str, metadata: Optional[Dict[str, Any]] = None):
        """Start tracking a pipeline stage"""
        self.stages[stage_name] = StageMetrics(
            name=stage_name,
            start_time=time.time(),
            metadata=metadata or {}
        )
        logger.debug(f"📊 Started stage: {stage_name}")
        
    def finish_stage(
        self,
        stage_name: str,
        count: int = 0,
        success: bool = True,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Finish tracking a pipeline stage"""
        if stage_name not in self.stages:
            logger.warning(f"Stage {stage_name} not found in metrics")
            return
            
        stage = self.stages[stage_name]
        stage.end_time = time.time()
        stage.count = count
        stage.success = success
        stage.error = error
        
        if metadata:
            stage.metadata.update(metadata)
            
        logger.debug(f"📊 Finished stage: {stage_name} ({stage.duration:.2f}s, {count} items)")
        
        if self.metrics_file:
            self._write_stage_metrics(stage)
            
    def get_stage_metrics(self, stage_name: str) -> Optional[StageMetrics]:
        """Get metrics for a specific stage"""
        return self.stages.get(stage_name)
        
    def get_total_time(self) -> float:
        """Get total pipeline execution time"""
        return time.time() - self.pipeline_start_time
        
    def get_total_throughput(self) -> float:
        """Get overall pipeline throughput"""
        total_items = sum(stage.count for stage in self.stages.values())
        total_time = self.get_total_time()
        
        if total_time > 0 and total_items > 0:
            return total_items / total_time
        return 0.0
        
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        total_items = sum(stage.count for stage in self.stages.values())
        successful_stages = sum(1 for stage in self.stages.values() if stage.success)
        
        return {
            'total_time': self.get_total_time(),
            'total_items': total_items,
            'total_throughput': self.get_total_throughput(),
            'stages_completed': len(self.stages),
            'stages_successful': successful_stages,
            'success_rate': successful_stages / len(self.stages) if self.stages else 0,
            'stages': {
                name: {
                    'duration': stage.duration,
                    'count': stage.count,
                    'throughput': stage.throughput,
                    'success': stage.success,
                    'error': stage.error
                }
                for name, stage in self.stages.items()
            }
        }
        
    def _write_stage_metrics(self, stage: StageMetrics):
        """Write stage metrics to TSV file"""
        if not self.metrics_file:
            return
            
        try:
            file_exists = self.metrics_file.exists()
            
            with open(self.metrics_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='\t')
                
                if not file_exists:
                    writer.writerow([
                        'timestamp', 'stage', 'duration', 'count', 'throughput',
                        'success', 'error', 'metadata'
                    ])
                
                writer.writerow([
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stage.start_time)),
                    stage.name,
                    f"{stage.duration:.3f}",
                    stage.count,
                    f"{stage.throughput:.2f}",
                    stage.success,
                    stage.error or "",
                    str(stage.metadata) if stage.metadata else ""
                ])
                
        except Exception as e:
            logger.warning(f"Failed to write metrics: {e}")
            
    def print_summary(self):
        """Print metrics summary to console"""
        summary = self.get_summary()
        
        print("\n" + "="*60)
        print("📊 PIPELINE METRICS SUMMARY")
        print("="*60)
        
        print(f"⏱️  Total Time: {summary['total_time']:.1f}s")
        print(f"📦 Total Items: {summary['total_items']:,}")
        print(f"🚀 Throughput: {summary['total_throughput']:.1f} items/sec")
        print(f"✅ Success Rate: {summary['success_rate']:.1%}")
        
        print(f"\n📋 STAGE BREAKDOWN")
        print("-" * 40)
        
        for stage_name, stage_data in summary['stages'].items():
            status = "✅" if stage_data['success'] else "❌"
            print(f"{status} {stage_name}:")
            print(f"   Duration: {stage_data['duration']:.2f}s")
            print(f"   Items: {stage_data['count']:,}")
            print(f"   Throughput: {stage_data['throughput']:.1f} items/sec")
            if stage_data['error']:
                print(f"   Error: {stage_data['error']}")
            print()
            
        target_throughput = 1000 / 60  # ~1 min/1000 prompts = 16.67 items/sec
        if summary['total_throughput'] >= target_throughput:
            print(f"🎯 PERFORMANCE TARGET MET! ({summary['total_throughput']:.1f} ≥ {target_throughput:.1f} items/sec)")
        else:
            print(f"⚠️  Performance below target ({summary['total_throughput']:.1f} < {target_throughput:.1f} items/sec)")
            
        print("="*60)
