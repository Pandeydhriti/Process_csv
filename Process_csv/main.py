import argparse
from argparse import Namespace

from generate_csv.csv_generator import CsvGenerator
from Process_csv_pl.csv_processor import CsvProcessor
from Process_csv_pl.chunk_processor import ChunkedCsvProcessor
from Process_csv_pl.multi_threaded_chunk_processor import MultithreadedCsvProcessor

def main():
    parser = argparse.ArgumentParser(description="CSV Processing Tool")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Sub-command to execute")

    # Sub-command for generating CSV
    generate_parser = subparsers.add_parser("generate", help="Generate a CSV file")
    generate_parser.add_argument('-s', type=int, help="Size of the CSV file (in MB)")
    generate_parser.add_argument('-r', type=int, help="Number of rows in the CSV file")
    generate_parser.add_argument('-c', type=int, default=100, help="Number of columns in each row (default: 100)")

    # Sub-command for processing CSV (basic)
    process_parser = subparsers.add_parser("process", help="Process a CSV file (basic)")
    process_parser.add_argument('-f', type=str, required=True, help="Path to the CSV file")

    # Sub-command for chunked processing
    chunked_parser = subparsers.add_parser("chunked", help="Process a CSV file in chunks")
    chunked_parser.add_argument('-f', type=str, required=True, help="Path to the CSV file")

    # Sub-command for multithreaded chunked processing
    multithreaded_parser = subparsers.add_parser("multithreaded", help="Process a CSV file using multithreading")
    multithreaded_parser.add_argument('-f', type=str, required=True, help="Path to the CSV file")

    # Parse arguments
    args: Namespace = parser.parse_args()

    # Execute the chosen sub-command
    if args.command == "generate":
        # Generate CSV
        if args.s is None and args.r is None:
            print("Error: Please provide either size (-s) or rows (-r) for the CSV file.")
            return
        generator = CsvGenerator(columns=args.c)
        generator.generate_csv(size=args.s, rows=args.r)

    elif args.command == "process":
        # Process CSV (basic processing)
        processor = CsvProcessor(filename=args.f)
        processor.process_csv()

    elif args.command == "chunked":
        # Process CSV with chunking
        chunk_processor = ChunkedCsvProcessor(filename=args.f)
        chunk_processor.process_in_chunks()

    elif args.command == "multithreaded":
        # Process CSV with multithreaded chunking
        multithreaded_processor = MultithreadedCsvProcessor(filename=args.f)
        multithreaded_processor.process_csv_multithreaded()

if __name__ == "__main__":
    main()