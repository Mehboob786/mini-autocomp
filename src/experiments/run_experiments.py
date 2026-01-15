import sys
import json
import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
sys.path.insert(0, SRC_DIR)


from table import LogStructuredTable
from workload import WorkloadGenerator
from simulator import Simulator

from compaction.naive import NaiveCompaction
from compaction.size_based import SizeBasedCompaction
from compaction.workload_aware import WorkloadAwareCompaction


import matplotlib.pyplot as plt

def load_workloads():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.abspath(
        os.path.join(base_dir, "..", "..", "data", "synthetic_workload.json")
    )

    with open(data_path) as f:
        return json.load(f)["workloads"]


def run_simulation(policy, workload_config):
    table = LogStructuredTable()
    workload = WorkloadGenerator(
        write_rate=workload_config["write_rate"],
        read_rate=workload_config["read_rate"]
    )

    sim = Simulator(table, workload, policy)

    for _ in range(workload_config["steps"]):
        sim.step()

    return sim.history



def plot_results(histories):
    # Create the results/plots directory if it doesn't exist
    base_dir = os.path.dirname(os.path.abspath(__file__))
    plots_dir = os.path.abspath(os.path.join(base_dir, "..", "..", "results", "plots"))
    os.makedirs(plots_dir, exist_ok=True)
    
    plt.figure()
    for name, history in histories.items():
        files = [h["files"] for h in history]
        plt.plot(files, label=name)

    plt.xlabel("Simulation Step")
    plt.ylabel("Number of Files")
    plt.title("File Count Over Time")
    plt.legend()
    plt.savefig(os.path.join(plots_dir, "file_count.png"))
    plt.close()

    plt.figure()
    for name, history in histories.items():
        latency = [h["latency"] for h in history]
        plt.plot(latency, label=name)

    plt.xlabel("Simulation Step")
    plt.ylabel("Estimated Query Latency")
    plt.title("Query Latency Over Time")
    plt.legend()
    plt.savefig(os.path.join(plots_dir, "query_latency.png"))
    plt.close()


if __name__ == "__main__":
    workloads = load_workloads()

    for wl in workloads:
        histories = {
            "Naive": run_simulation(NaiveCompaction(), wl),
            "Size-Based": run_simulation(SizeBasedCompaction(), wl),
            "Workload-Aware": run_simulation(WorkloadAwareCompaction(), wl)
        }

        plot_results(histories)
        print(f"Completed workload: {wl['name']}")