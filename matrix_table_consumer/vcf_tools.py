import os
import sys
import argparse
import ctypes
from datetime import datetime


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


class VCFTools:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-filter", required=False, action="store_true", help="Filter VCF by expression."
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
