from concurrent.futures import ThreadPoolExecutor, as_completed
import polars as pl
import time
import os
import psutil
from Process_csv_pl.utils import CsvUtils

class MultithreadedCsvProcessor:
    def __init__(self, filename,memory_fraction=0.8):
        """
        Initialize the MultithreadedCsvProcessor with the path to the CSV file.
        :param filename: Path to the CSV file to process.
        """
        self.filename = filename
        self.csv_utils = CsvUtils(memory_fraction)

    @staticmethod
    def process_batch(batch):
        """
        Calculate the sum of all numeric values in a batch.
        :param batch: A Polars DataFrame batch.
        :return: The sum of all numeric values in the batch.
        """
        # Sum of all columns in the batch
        batch_col_sum = batch.select(pl.all().sum())
        # Get the first row containing the column sums
        sum_col = batch_col_sum.row(0)
        # Total sum
        batch_sum = sum(sum_col)
        return batch_sum

    def process_csv_multithreaded(self):
        """
        Process the CSV file using multithreading to compute the total sum of all numeric values.
        """
        try:
            # Track start time and memory
            start_time = time.time()
            process = psutil.Process(os.getpid())
            mem_before = process.memory_info().rss

            # Determine the number of threads based on system resources
            num_threads = min(4, psutil.cpu_count(logical=False))  # Logical=False means physical cores

            # Get batch size based on system memory
            batch_size = self.csv_utils.get_chunk_size(self.filename, num_threads)

            # Read CSV file in batches
            reader = pl.read_csv_batched(self.filename, batch_size=batch_size, has_header=False)
            futures = []
            total_sum = 0

            # Thread pool executor for parallel processing
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                while True:
                    batch_list = reader.next_batches(num_threads)
                    if not batch_list:
                        break  # Exit if no more batches
                    # Submit each batch to the thread pool for processing
                    for batch in batch_list:
                        futures.append(executor.submit(self.process_batch, batch))
                # Process completed batches as they finish
                for f in as_completed(futures):
                    total_sum += f.result()

            # Track end time and memory
            end_time = time.time()
            mem_after = process.memory_info().rss

            # Output processing details
            print(f'Start Time        : {time.ctime(start_time)}')
            print(f'End Time          : {time.ctime(end_time)}')
            print(f'Time Spent        : {end_time - start_time:.2f} seconds')
            print(f'File Size         : {os.path.getsize(self.filename) / (1024 * 1024):.2f} MB')
            print(f'Memory Used       : {(mem_after - mem_before) / (1024 * 1024):.2f} MB')
            print(f'Total Sum of CSV  : {total_sum}')
            print(f'Number of Threads : {num_threads}')
        except FileNotFoundError:
            print(f"Error: File '{self.filename}' not found.")
        except pl.exceptions.PolarsError as e:
            print(f"Error reading CSV with Polars: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")