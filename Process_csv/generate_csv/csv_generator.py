import csv
import random
import os
import datetime

class CsvGenerator:
    def __init__(self, columns=100):
        """
        Initialize the CSV generator with the number of columns.
        :param columns: Number of columns in the generated CSV.
        """
        self.columns = columns

    def _generate_random_row(self):
        """
        Generate a random row with the specified number of columns.
        :return: A list of random integers.
        """
        return [random.randint(1, 1000) for _ in range(self.columns)]

    def generate_csv(self, size=None, rows=None):
        """
        Generate a CSV file with the specified size or number of rows.
        :param size: Size of the CSV file in MB.
        :param rows: Number of rows in the CSV file.
        """
        if rows is None and size is None:
            print('Please provide size or rows')
            return

        name = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'{name}.csv'
        with open(filename, 'w', newline="") as csvfile:
            writer = csv.writer(csvfile)

            if rows:
                for _ in range(rows):
                    row = self._generate_random_row()
                    writer.writerow(row)

            elif size:
                target_size = size * 1024 * 1024
                while os.path.getsize(filename) < target_size:
                    row = self._generate_random_row()
                    writer.writerow(row)
        print(f'Generated {name} successfully')