from abc import ABC, abstractmethod

class CompactionPolicy(ABC):
    @abstractmethod
    def should_compact(self, table, workload_stats):
        pass

    @abstractmethod
    def select_files(self, table):
        pass
