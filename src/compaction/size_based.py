from .base import CompactionPolicy

class SizeBasedCompaction(CompactionPolicy):
    def __init__(self, max_size_mb=50):
        self.max_size_mb = max_size_mb

    def should_compact(self, table, workload_stats):
        small_files = [f for f in table.files if f.size_mb < self.max_size_mb]
        return len(small_files) > 5

    def select_files(self, table):
        return [f.id for f in table.files if f.size_mb < self.max_size_mb]
