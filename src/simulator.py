from metrics import estimate_query_latency

class Simulator:
    def __init__(self, table, workload, policy):
        self.table = table
        self.workload = workload
        self.policy = policy
        self.history = []

    def step(self):
        write_size = self.workload.generate_write()
        self.table.append_write(write_size)

        reads = self.workload.generate_reads()
        latency = estimate_query_latency(
            self.table.file_count(),
            sum(f.size_mb for f in self.table.files)
        )

        workload_stats = {"reads": reads}

        if self.policy.should_compact(self.table, workload_stats):
            files = self.policy.select_files(self.table)
            self.table.compact(files)

        self.history.append({
            "files": self.table.file_count(),
            "latency": latency
        })
