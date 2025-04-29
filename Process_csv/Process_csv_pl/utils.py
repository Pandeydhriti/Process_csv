import polars as pl
import psutil

class CsvUtils:
    def __init__(self, memory_fraction=0.8):
        """
        Initialize the CsvUtils class with a memory fraction for chunk size calculation.
        :param memory_fraction: Fraction of available memory to be used for processing.
        """
        self.memory_fraction = memory_fraction

    @staticmethod
    def estimate_row_size(filepath, sample_row=100):
        """
        Estimate the average size of a single row in the CSV file.
        :param filepath: Path to the CSV file.
        :param sample_row: Number of rows to sample for size estimation.
        :return: Estimated size of a single row in bytes.
        """
        # Read csv file
        df = pl.read_csv(filepath, n_rows=sample_row)

        # Get size of sample rows in bytes
        size_bytes = df.estimated_size()

        # Get size of one row and return
        row_size = size_bytes // sample_row
        return size_bytes

    def get_chunk_size(self, filepath, num_threads=1):
        """
        Calculate chunk size (number of rows) for processing CSV files.
        :param filepath: Path to the CSV file.
        :param num_threads: Number of threads to use for processing.
        :return: Chunk size in number of rows.
        """
        try:
            # Estimate size of one row
            row_size = self.estimate_row_size(filepath)

            # Get available system memory
            available_memory = psutil.virtual_memory().available

            # Use only a fraction of the available memory
            usable_memory = int(available_memory * self.memory_fraction)

            # Divide memory by number of threads
            memory_per_thread = usable_memory // num_threads

            # Calculate how many rows fit into memory per thread
            chunk_rows = memory_per_thread // row_size

            # Keep chunk size reasonable
            return max(10_000, min(chunk_rows, 100_000))  # max is 100,000 rows and min is 10,000 rows
        except Exception as e:
            print(f"Error calculating chunk size: {e}")
            raise