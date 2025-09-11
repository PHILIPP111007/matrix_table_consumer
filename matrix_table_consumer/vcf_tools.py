import os
import sys
import argparse
import ctypes
from datetime import datetime

from bio2zarr import vcf as vcf2zarr

from matrix_table_consumer.functions_py.sort import sort_vcf


current_dir = os.path.dirname(__file__)
library_path = os.path.join(current_dir, "main.so")

lib = ctypes.CDLL(library_path)
Filter = lib.Filter
Merge = lib.Merge
View = lib.View

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

View.argtypes = [
    ctypes.c_char_p,
]
View.restype = None


def get_time() -> str:
    return datetime.now().strftime("%d-%m-%Y %H:%M:%S")


def logger_info(s: str) -> None:
    t = get_time()
    print(f"[{t}] - INFO - {s}")


def logger_error(s: str) -> None:
    t = get_time()
    print(f"[{t}] - ERROR - {s}")


def filter(include: str, input_vcf: str, output_vcf: str, num_cpu: int) -> None:
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
    vcf1: str = "",
    vcf2: str = "",
    output_vcf: str = "",
    file_with_vcfs: str = ".",
) -> None:
    if vcf1 and not os.path.exists(vcf1):
        logger_error("Input vcf not found")
        sys.exit(1)

    if vcf2 and not os.path.exists(vcf2):
        logger_error("Input vcf not found")
        sys.exit(1)

    if file_with_vcfs != "." and not os.path.exists(file_with_vcfs):
        logger_error("Input vcf not found")
        sys.exit(1)

    vcf1_encoded = vcf1.encode("utf-8")
    vcf2_encoded = vcf2.encode("utf-8")
    output_vcf_encoded = output_vcf.encode("utf-8")
    file_with_vcfs_encoded = file_with_vcfs.encode("utf-8")

    Merge(vcf1_encoded, vcf2_encoded, output_vcf_encoded, file_with_vcfs_encoded)


def view(vcf_path: str):
    if vcf_path and not os.path.exists(vcf_path):
        logger_error("Input vcf not found")
        sys.exit(1)

    vcf_encoded = vcf_path.encode("utf-8")
    View(vcf_encoded)


def save_vcf_as_zarr(vcf_path: str, output_vcz: str, num_cpu: int, show_progress: bool):
    if vcf_path and not os.path.exists(vcf_path):
        logger_error("Input vcf not found")
        sys.exit(1)

    vcf2zarr.convert(
        vcfs=[vcf_path],
        vcz_path=output_vcz,
        worker_processes=num_cpu,
        show_progress=show_progress,
    )


def sort(vcf_path: str, output_vcf: str):
    if not os.path.exists(vcf_path):
        logger_error("Input vcf not found")
        sys.exit(1)

    sort_vcf(input_vcf=vcf_path, output_vcf=output_vcf)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-filter", required=False, action="store_true", help="Filter VCF by expression."
    )
    parser.add_argument(
        "-merge", required=False, action="store_true", help="Merge VCF files."
    )
    parser.add_argument(
        "-sort", required=False, action="store_true", help="Sort VCF file."
    )
    parser.add_argument(
        "-save_vcf_as_zarr",
        required=False,
        action="store_true",
        help="Save VCF in zarr format (.vcz).",
    )
    parser.add_argument(
        "-view",
        required=False,
        action="store_true",
        help="View VCF file. j -> next line, k -> previous line, ENTER -> next page, / -> enter line number, q -> quit.",
    )
    parser.add_argument(
        "-i",
        "--include",
        required=False,
        type=str,
        help="Expression. Example: QUAL>=30 or FILTER=='PASS' (string values must be under quotes).",
    )
    parser.add_argument(
        "-vcf", "--vcf", required=False, type=str, help="Input VCF file."
    )
    parser.add_argument(
        "-vcf2", "--vcf2", required=False, type=str, help="Input VCF file."
    )
    parser.add_argument(
        "-file_with_vcfs",
        "--file_with_vcfs",
        required=False,
        type=str,
        default=".",
        help="File contains vcf paths which are located on separate lines in the file.",
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
    parser.add_argument(
        "-show_progress", required=False, action="store_true", help="Show progress."
    )

    args = parser.parse_args()

    if len(sys.argv) > 1:
        if args.filter:
            include: str = args.include
            input_vcf: str = args.vcf
            output_vcf: str = args.output
            num_cpu: int = args.num_cpu

            if include and input_vcf and output_vcf:
                filter(
                    include=include,
                    input_vcf=input_vcf,
                    output_vcf=output_vcf,
                    num_cpu=num_cpu,
                )
            else:
                logger_error("Provide args")
        elif args.merge:
            vcf1: str = args.vcf
            vcf2: str = args.vcf2
            output_vcf: str = args.output
            file_with_vcfs: str = args.file_with_vcfs

            if (vcf1 and vcf2 or file_with_vcfs != ".") and output_vcf:
                merge(
                    vcf1=vcf1,
                    vcf2=vcf2,
                    output_vcf=output_vcf,
                    file_with_vcfs=file_with_vcfs,
                )
            else:
                logger_error("Provide args")
        elif args.view:
            vcf_path: str = args.vcf

            if vcf_path:
                view(vcf_path=vcf_path)
            else:
                logger_error("Provide args")
        elif args.save_vcf_as_zarr:
            vcf_path: str = args.vcf
            output_vcz: str = args.output
            num_cpu: int = args.num_cpu
            show_progress: bool = args.show_progress

            if vcf_path and output_vcz:
                save_vcf_as_zarr(
                    vcf_path=vcf_path,
                    output_vcz=output_vcz,
                    num_cpu=num_cpu,
                    show_progress=show_progress,
                )
            else:
                logger_error("Provide args")
        elif args.sort:
            vcf_path: str = args.vcf_path
            output_vcf: str = args.output
            if vcf_path and output_vcf:

                sort(vcf_path=vcf_path, output_vcf=output_vcf)
            else:
                logger_error("Provide args")

    else:
        logger_error("Provide args")


if __name__ == "__main__":
    main()
