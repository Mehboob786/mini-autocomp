from table import LogStructuredTable
from workload import WorkloadGenerator
from compaction.workload_aware import WorkloadAwareCompaction
from simulator import Simulator

table = LogStructuredTable()
workload = WorkloadGenerator(write_rate=5, read_rate=20)
policy = WorkloadAwareCompaction()

sim = Simulator(table, workload, policy)

for _ in range(100):
    sim.step()

print(sim.history[-5:])
