import csv
import os
import psutil
import time
import polars as pl

class CsvProcessor:
    def __init__(self, filename):
        """
        Initialize the CsvProcessor with the path to the CSV file.
        :param filename: Path to the CSV file to process.
        """
        self.filename = filename

    def process_csv(self):
        """
        Process the CSV file to compute the sum of all its rows and columns.
        Prints the processing time, memory usage, and total sum.
        """
        try:
            start_time = time.time()

            # Track memory usage
            process = psutil.Process(os.getpid())
            mem_before = process.memory_info().rss

            # Read CSV file using Polars
            df = pl.read_csv(self.filename, has_header=False)

            # Select all columns and compute their sum individually
            sum_df = df.select(
                pl.all().sum()
            )

            # Get the first row
            sum_row = sum_df.row(0)

            # Sum all the rows
            total_sum = sum(sum_row)

            # Record end time and memory usage
            end_time = time.time()
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