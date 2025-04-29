import time
import os
import psutil
import polars as pl
from Process_csv_pl.utils import CsvUtils

class ChunkedCsvProcessor:
    def __init__(self, filename, memory_fraction=0.8):
        """
        Initialize the ChunkedCsvProcessor with the path to the CSV file.
        :param filename: Path to the CSV file to process.
        """
        self.filename = filename
        self.csv_utils=CsvUtils(memory_fraction)

    def process_in_chunks(self):
        """
        Read a large CSV file in chunks and compute the total sum of all numeric values.
        """
        try:
            # Start time
            start_time = time.time()

            # Track initial memory usage
            process = psutil.Process(os.getpid())
            mem_before = process.memory_info().rss

            # Calculate chunk size based on available memory
            chunk_size = self.csv_utils.get_chunk_size(self.filename)

            total_sum = 0

            # Read CSV in batches
            reader = pl.read_csv_batched(self.filename, batch_size=chunk_size, has_header=False)

            while True:
                batch = reader.next_batches(1)
                if not batch:
                    break  # If no more batches, break the loop

                # Column-wise sum of the batch
                df_batch = batch[0].select(pl.all().sum())
                # Get the first row
                batch_row = df_batch.row(0)
                # Update total sum
                total_sum += sum(batch_row)

            # End time
            end_time = time.time()
            # Track final memory usage
            mem_after = process.memory_info().rss

            # Output processing details
            print(f'Start Time        : {time.ctime(start_time)}')
            print(f'End Time          : {time.ctime(end_time)}')
            print(f'Time Spent        : {end_time - start_time:.2f} seconds')
            print(f'File Size         : {os.path.getsize(self.filename) / (1024 * 1024):.2f} MB')
            print(f'Memory Used       : {(mem_after - mem_before) / (1024 * 1024):.2f} MB')
            print(f'Total Sum of CSV  : {total_sum}')
        except FileNotFoundError:
            print(f"Error: File '{self.filename}' not found.")
        except pl.exceptions.PolarsError as e:
            print(f"Error reading CSV with Polars: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")