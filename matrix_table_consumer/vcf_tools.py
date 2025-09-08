import os
import sys
import argparse
import ctypes
from datetime import datetime


current_dir = os.path.dirname(__file__)
library_path = os.path.join(current_dir, "main.so")

lib = ctypes.CDLL(library_path)
Filter = lib.Filter
Merge = lib.Merge

Filter.argtypes = [
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_int,
]
Filter.restype = None

Merge.argtypes = [
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
]
Merge.restype = None


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
        self, include: str, input_vcf: str, output_vcf: str, num_cpu: int
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

        Filter(include_encoded, input_vcf_encoded, output_vcf_encoded, num_cpu)

    def merge(
        self,
        vcf1: str,
        vcf2: str,
        output_vcf: str,
    ) -> None:
        if not os.path.exists(vcf1):
            logger_error("Input vcf not found")
            sys.exit(1)

        if not os.path.exists(vcf2):
            logger_error("Input vcf not found")
            sys.exit(1)

        vcf1__encoded = vcf1.encode("utf-8")
        vcf2__encoded = vcf2.encode("utf-8")
        output_vcf_encoded = output_vcf.encode("utf-8")

        Merge(vcf1__encoded, vcf2__encoded, output_vcf_encoded)


def main():
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
            num_cpu: bool = args.num_cpu

            if include and input_vcf and output_vcf:
                vcftools = VCFTools()
                vcftools.filter(
                    include=include,
                    input_vcf=input_vcf,
                    output_vcf=output_vcf,
                    num_cpu=num_cpu,
                )
            else:
                logger_error("Provide args")
        elif sys.argv[1] == "-merge":
            vcf1: str = args.vcf
            vcf2: str = args.vcf2
            output_vcf: str = args.output

            if vcf1 and vcf2 and output_vcf:
                vcftools = VCFTools()
                vcftools.merge(
                    vcf1=vcf1,
                    vcf2=vcf2,
                    output_vcf=output_vcf,
                )
            else:
                logger_error("Provide args")


if __name__ == "__main__":
    main()
