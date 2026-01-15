import random

class WorkloadGenerator:
    def __init__(self, write_rate, read_rate):
        self.write_rate = write_rate
        self.read_rate = read_rate

    def generate_write(self):
        """Simulate small streaming writes"""
        return random.uniform(1, 10)  # MB

    def generate_reads(self):
        """Simulate number of read queries"""
        return random.randint(0, self.read_rate)
