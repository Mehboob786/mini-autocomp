from .base import CompactionPolicy

class NaiveCompaction(CompactionPolicy):
    def __init__(self, threshold=50):
        self.threshold = threshold

    def should_compact(self, table, workload_stats):
        return table.file_count() > self.threshold

    def select_files(self, table):
        return [f.id for f in table.files]
