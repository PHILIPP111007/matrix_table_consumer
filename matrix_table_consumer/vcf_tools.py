import os
import sys
import argparse
import ctypes
from datetime import datetime
import curses
import gzip


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


def lazy_read(file_path: str):
    """Lazy loading of strings from a file."""

    if file_path.endswith(".gz"):
        with gzip.open(file_path, "r") as f:
            yield from f
    else:
        with open(file_path, "r") as f:
            yield from f


def view(vcf: str):
    def show_file(stdscr):
        stdscr.clear()
        curses.curs_set(0)  # Hiding the cursor
        screen_height, _ = stdscr.getmaxyx()
        buffer_size = screen_height * 2  # Buffer zone size for comfortable scrolling
        line_buffer = []
        generator = lazy_read(vcf)
        position = 0
        max_position = None

        while True:
            stdscr.clear()

            while len(line_buffer) <= buffer_size and not max_position:
                try:
                    next_line = next(generator).strip()
                    line_buffer.append(next_line)
                except StopIteration:
                    max_position = len(line_buffer) - screen_height

            # Checking the boundaries of the position
            if position < 0:
                position = 0
            elif position > max_position:
                position = max_position or 0

            # Displaying lines on the screen
            for idx in range(screen_height):
                if position + idx < len(line_buffer):
                    stdscr.addstr(idx, 0, line_buffer[position + idx])

            # Waiting for input event
            key = stdscr.getch()
            if key == ord("k"):  # Up arrow
                position -= 1
            elif key == ord("j"):  # Down arrow
                position += 1
            elif key == ord("\n"):  # Move page down
                position += screen_height
            elif key == ord("/"):  # Go to the specified line
                stdscr.clear()
                curses.echo()  # Turn on echo mode to display input
                stdscr.addstr(0, 0, "Enter line number: ")
                input_str = stdscr.getstr().decode("utf-8")
                try:
                    new_pos = int(input_str.strip()) - 1
                    if new_pos >= 0:
                        position = min(new_pos, len(line_buffer))
                except ValueError:
                    pass
                finally:
                    curses.noecho()  # Turn off echo mode
            elif key == ord("q"):
                break

    curses.wrapper(show_file)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-filter", required=False, action="store_true", help="Filter VCF by expression."
    )
    parser.add_argument(
        "-merge", required=False, action="store_true", help="Merge VCF files."
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

    args = parser.parse_args()

    if len(sys.argv) > 1:
        if args.filter:
            include: str = args.include
            input_vcf: str = args.vcf
            output_vcf: str = args.output
            num_cpu: bool = args.num_cpu

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
            vcf: str = args.vcf

            if vcf:
                view(vcf=vcf)
            else:
                logger_error("Provide args")
    else:
        logger_error("Provide args")


if __name__ == "__main__":
    main()
