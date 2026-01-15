import time
import uuid

class DataFile:
    def __init__(self, size_mb):
        self.id = str(uuid.uuid4())
        self.size_mb = size_mb
        self.created_at = time.time()

class LogStructuredTable:
    def __init__(self):
        self.files = []

    def append_write(self, size_mb):
        """Append-only write"""
        self.files.append(DataFile(size_mb))

    def compact(self, file_ids):
        """Merge selected files into one"""
        selected = [f for f in self.files if f.id in file_ids]
        if not selected:
            return 0

        total_size = sum(f.size_mb for f in selected)

        # Remove old files
        self.files = [f for f in self.files if f.id not in file_ids]

        # Add new compacted file
        self.files.append(DataFile(total_size))
        return total_size

    def file_count(self):
        return len(self.files)

    def avg_file_size(self):
        if not self.files:
            return 0
        return sum(f.size_mb for f in self.files) / len(self.files)
