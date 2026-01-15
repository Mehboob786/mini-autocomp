from .base import CompactionPolicy

class WorkloadAwareCompaction(CompactionPolicy):
    def __init__(self, read_threshold=10):
        self.read_threshold = read_threshold

    def should_compact(self, table, workload_stats):
        high_reads = workload_stats["reads"] > self.read_threshold
        many_files = table.file_count() > 20
        return high_reads and many_files

    def select_files(self, table):
        # Prioritize smallest files first
        sorted_files = sorted(table.files, key=lambda f: f.size_mb)
        return [f.id for f in sorted_files[:10]]
