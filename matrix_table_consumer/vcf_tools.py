import os
import sys
import argparse
import ctypes
from datetime import datetime
from collections import defaultdict


current_dir = os.path.dirname(__file__)
library_path = os.path.join(current_dir, "main.so")

lib = ctypes.CDLL(library_path)
Filter = lib.Filter

Filter.argtypes = [
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_bool,
    ctypes.c_int,
]
Filter.restype = None


def get_time() -> str:
    return datetime.now().strftime("%d-%m-%Y %H:%M:%S")


def logger_info(s: str) -> None:
    t = get_time()
    print(f"[{t}] - INFO - {s}")


def logger_error(s: str) -> None:
    t = get_time()
    print(f"[{t}] - ERROR - {s}")


class VCFRecord:
    def __init__(self, chrom, pos, id, ref, alt, qual, filter, info):
        self.chrom: str = chrom
        self.pos = pos
        self.id = id
        self.ref = ref
        self.alt = alt
        self.qual = qual
        self.filter = filter
        self.info = info
        self.samples = {}

    def add_sample(self, sample_name: str, sample_value: str):
        self.samples[sample_name] = sample_value

    def __repr__(self):
        return f"{self.chrom}:{self.pos}"


class VCFTools:
    def parse_vcf_line(self, line: str, sample_names: list[str]) -> VCFRecord:
        parts = line.strip().split("\t")
        chrom = parts[0]
        pos = parts[1]
        id = parts[2]
        ref = parts[3]
        alt = parts[4]
        qual = parts[5]
        filter = parts[6]
        info = parts[7]
        record = VCFRecord(chrom, pos, id, ref, alt, qual, filter, info)
        samples_values = parts[9:]
        for sample_name, sample_value in zip(sample_names, samples_values):
            record.add_sample(sample_name, sample_value)
        return record

    def read_vcf_headers(self, vcf1: str, vcf2: str) -> list[str]:
        with open(vcf1, "r") as file1, open(vcf2, "r") as file2:
            headers: list[str] = []
            samples_names = []
            other_header = []

            for line in file1:
                if line.startswith("##"):
                    if line not in headers:
                        headers.append(line)
                elif line.startswith("#CHROM"):
                    header_end = line.strip().split("\t")
                    other_header = header_end[:9]
                    samples_names = header_end[9:]

            for line in file2:
                if line.startswith("##"):
                    if line not in headers:
                        headers.append(line)
                elif line.startswith("#CHROM"):
                    header_end = line.strip().split("\t")
                    samples_names += header_end[9:]

            other_header = "\t".join(other_header) + "\t"
            headers.append(other_header)
            samples_names = sorted(samples_names)
            samples_names_str = "\t".join(samples_names).strip() + "\n"
            headers.append(samples_names_str)
            return headers

    def read_vcf(self, file_name: str) -> list[VCFRecord]:
        records = []
        sample_names = []
        with open(file_name, "r") as file:
            for line in file:
                if line.startswith("#CHROM"):
                    sample_names = line.strip().split("\t")[9:]
                    continue
                elif line.startswith("#"):
                    continue
                elif len(line.strip()) == 0:
                    break
                record = self.parse_vcf_line(line, sample_names)
                records.append(record)
        return records

    def merge_records(self, records1: list[VCFRecord], records2: list[VCFRecord]):
        merged_records = defaultdict(list)
        all_samples = set()

        for rec in records1:
            key = (rec.chrom, rec.pos)
            merged_records[key].append(rec)

            all_samples.update(rec.samples.keys())

        for rec in records2:
            key = (rec.chrom, rec.pos)
            merged_records[key].append(rec)
            all_samples.update(rec.samples.keys())

        sorted_keys: list[tuple[str, str]] = sorted(
            merged_records.keys(), key=lambda k: (k[0], k[1])
        )
        merged_results = []
        for key in sorted_keys:
            entries = merged_records[key]
            first_entry = entries[0]
            combined_record = VCFRecord(
                first_entry.chrom,
                str(key[1]),
                first_entry.id,
                first_entry.ref,
                first_entry.alt,
                first_entry.qual,
                first_entry.filter,
                first_entry.info,
            )
            combined_samples = {}
            for entry in entries:
                combined_samples.update(entry.samples)
            combined_record.samples = combined_samples
            merged_results.append(combined_record)
        return merged_results, list(all_samples)

    def write_headers(self, header_lines: list[str], output_file: str):
        with open(output_file, "w") as file:
            for line in header_lines:
                file.write(line)

    def write_merged_records(
        self, merged_records: list[VCFRecord], samples_ordered, output_file
    ):
        with open(output_file, "a") as file:
            for rec in merged_records:
                columns = [
                    rec.chrom,
                    str(rec.pos),
                    rec.id,
                    rec.ref,
                    rec.alt,
                    rec.qual,
                    rec.filter,
                    rec.info,
                ]
                sample_values = []
                for sample in samples_ordered:
                    if sample in rec.samples:
                        sample_data = rec.samples[sample]
                    else:
                        sample_data = "."
                    sample_values.append(sample_data)
                columns.extend(sample_values)
                file.write("\t".join(columns) + "\n")

    def filter(
        self, include: str, input_vcf: str, output_vcf: str, is_gzip: bool, num_cpu: int
    ) -> None:
        if not os.path.exists(input_vcf):
            logger_error("Input vcf not found")
            sys.exit(1)

        if os.path.exists(output_vcf):
            logger_error("File output vcf already exists")
            sys.exit(1)

        include_encoded = include.encode("utf-8")
        input_vcf_encoded = input_vcf.encode("utf-8")
        output_vcf_encoded = output_vcf.encode("utf-8")

        Filter(include_encoded, input_vcf_encoded, output_vcf_encoded, is_gzip, num_cpu)

    def merge(
        self, vcf1: str, vcf2: str, output_vcf: str, is_gzip: bool, num_cpu: int
    ) -> None:
        if not os.path.exists(vcf1):
            logger_error("Input vcf not found")
            sys.exit(1)

        if not os.path.exists(vcf2):
            logger_error("Input vcf not found")
            sys.exit(1)

        headers = self.read_vcf_headers(vcf1, vcf2)

        records_1 = self.read_vcf(vcf1)
        records_2 = self.read_vcf(vcf2)

        merged_records, all_samples = self.merge_records(records_1, records_2)
        self.write_headers(headers, output_vcf)
        self.write_merged_records(merged_records, sorted(all_samples), output_vcf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-filter", required=False, action="store_true", help="Filter VCF by expression."
    )
    parser.add_argument(
        "-merge", required=False, action="store_true", help="Merge VCF files."
    )
    parser.add_argument(
        "-i",
        "--include",
        required=False,
        type=str,
        help="Expression. Example: 'QUAL >= 30'",
    )
    parser.add_argument(
        "-vcf", "--vcf", required=False, type=str, help="Input VCF file."
    )
    parser.add_argument(
        "-vcf2", "--vcf2", required=False, type=str, help="Input VCF file."
    )
    parser.add_argument(
        "-o", "--output", required=False, type=str, help="Output VCF file."
    )
    parser.add_argument("-gzip", required=False, action="store_true", help="Is gzip.")
    parser.add_argument(
        "-num_cpu",
        "--num_cpu",
        type=int,
        required=False,
        default=1,
        help="Number CPUs.",
    )

    args = parser.parse_args()

    if len(sys.argv) > 1:
        if sys.argv[1] == "-filter":
            include: str = args.include
            input_vcf: str = args.vcf
            output_vcf: str = args.output
            is_gzip: bool = args.gzip
            num_cpu: bool = args.num_cpu

            if include and input_vcf and output_vcf:
                vcftools = VCFTools()
                vcftools.filter(
                    include=include,
                    input_vcf=input_vcf,
                    output_vcf=output_vcf,
                    is_gzip=is_gzip,
                    num_cpu=num_cpu,
                )
            else:
                logger_error("Provide args")
        elif sys.argv[1] == "-merge":
            vcf1: str = args.vcf
            vcf2: str = args.vcf2
            output_vcf: str = args.output
            is_gzip: bool = args.gzip
            num_cpu: bool = args.num_cpu

            if vcf1 and vcf2 and output_vcf:
                vcftools = VCFTools()
                vcftools.merge(
                    vcf1=vcf1,
                    vcf2=vcf2,
                    output_vcf=output_vcf,
                    is_gzip=is_gzip,
                    num_cpu=num_cpu,
                )
            else:
                logger_error("Provide args")
