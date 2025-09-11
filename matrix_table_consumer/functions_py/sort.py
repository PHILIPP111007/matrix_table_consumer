import gzip
import os
import tempfile
from typing import List, Tuple, Iterator

from .logger import logger_info, logger_error


def sort_vcf(input_vcf: str, output_vcf: str, chunk_size: int = 1_000_000):
    def chromosome_key(chrom: str) -> tuple[int, int]:
        chrom = chrom.upper()
        if chrom.startswith("CHR"):
            chrom = chrom[3:]

        try:
            return (0, int(chrom))
        except ValueError:
            special_chroms = {"X": 100, "Y": 101, "MT": 102, "M": 102}
            return (1, special_chroms.get(chrom, 1000 + hash(chrom)))

    def read_chunk(iterator: Iterator[str], size: int) -> List[Tuple[str, int, str]]:
        """Reads a chunk of records of a given size"""

        chunk = []
        for _ in range(size):
            try:
                line = next(iterator)
                if not line.startswith("#"):
                    parts = line.strip().split("\t")
                    if len(parts) >= 2:
                        chrom, pos = parts[0], int(parts[1])
                        chunk.append((chrom, pos, line))
            except StopIteration:
                break
        return chunk

    def write_chunk(chunk: List[Tuple[str, int, str]], filename: str):
        """Writes the sorted chunk to a temporary file"""

        chunk.sort(key=lambda x: (chromosome_key(x[0]), x[1]))
        with open(filename, "w") as f:
            for _, _, line in chunk:
                f.write(line)

    def merge_sorted_files(file_paths: List[str], output_file: str):
        """Merge sorted temporary files into one"""

        # Open all files for reading
        files = [open(f, "r") for f in file_paths]
        current_lines = [f.readline() for f in files]

        with open(output_file, "w") as out:
            while any(current_lines):
                # Finding the minimal entry
                min_index = -1
                min_record = None
                min_key = None

                for i, line in enumerate(current_lines):
                    if not line:
                        continue

                    parts = line.strip().split("\t")
                    if len(parts) >= 2:
                        chrom, pos = parts[0], int(parts[1])
                        key = (chromosome_key(chrom), pos)

                        if min_key is None or key < min_key:
                            min_key = key
                            min_record = line
                            min_index = i

                if min_index != -1:
                    out.write(min_record)
                    current_lines[min_index] = files[min_index].readline()

        # Close all files
        for f in files:
            f.close()

    try:
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()
        logger_info(f"Temp dir: {temp_dir}")
        temp_files = []

        open_func = gzip.open if input_vcf.endswith(".gz") else open
        open_mode = "rt" if input_vcf.endswith(".gz") else "r"

        # Read the headers
        header_lines = []
        with open_func(input_vcf, open_mode) as f:
            for line in f:
                if line.startswith("#"):
                    header_lines.append(line)
                else:
                    break

        # Processing the file in chunks
        chunk_count = 0
        with open_func(input_vcf, open_mode) as f:
            for line in f:
                if not line.startswith("#"):
                    f.seek(0)
                    for _ in header_lines:
                        next(f)
                    break
            data_iterator = iter(f)

            while True:
                chunk = read_chunk(data_iterator, chunk_size)
                if not chunk:
                    break

                # Sort and save the chunk
                temp_file = os.path.join(temp_dir, f"chunk_{chunk_count}.tmp")
                write_chunk(chunk, temp_file)
                logger_info(f"Saved chunk in {temp_file}")
                temp_files.append(temp_file)
                chunk_count += 1

        # If the file is small and fits into one chunk
        if len(temp_files) == 1:
            with open(output_vcf, "w") as out, open(temp_files[0], "r") as temp:
                out.writelines(header_lines)
                out.writelines(temp)
        else:
            # Deleting temporary files
            merged_temp = os.path.join(temp_dir, "merged.tmp")
            merge_sorted_files(temp_files, merged_temp)

            # We write the result with headings
            with open(output_vcf, "w") as out, open(merged_temp, "r") as temp:
                out.writelines(header_lines)
                out.writelines(temp)

        # Clearing temporary files
        for temp_file in temp_files:
            os.remove(temp_file)
        if os.path.exists(merged_temp):
            os.remove(merged_temp)
        os.rmdir(temp_dir)

        logger_info(f"Successfully sorted {chunk_count} chunks")

    except Exception as e:
        logger_error(f"Error: {e}")
        try:
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
        except:
            pass
