# cython: language_level=3, boundscheck=False, wraparound=False, initializedcheck=False, cdivision=True, infer_types=True

import cython
import curses
import gzip


def lazy_read(file_path: str):
    """Lazy loading of strings from a file."""

    if file_path.endswith(".gz"):
        with gzip.open(file_path, "r") as f:
            yield from f
    else:
        with open(file_path, "r") as f:
            yield from f


def view_c(vcf: str):
    def show_file(stdscr):
        stdscr.clear()
        curses.curs_set(0)
        screen_height: cython.int
        width: cython.int

        screen_height, width = stdscr.getmaxyx()
        buffer_size: cython.int = screen_height * 2
        line_buffer: list = []
        generator = lazy_read(vcf)
        position: cython.long = 0
        max_position: cython.long = -1

        while True:
            stdscr.clear()

            while len(line_buffer) <= buffer_size and max_position == -1:
                try:
                    next_line = next(generator).strip()
                    line_buffer.append(next_line)
                except StopIteration:
                    max_position = len(line_buffer) - screen_height

            buffer_size += screen_height * 2

            if position < 0:
                position = 0
            elif max_position != -1 and position > max_position:
                position = max_position

            idx: cython.long
            for idx in range(screen_height):
                y: cython.long = idx
                x: cython.long = 0
                if position + idx < len(line_buffer):
                    text_to_display: str | bytes = line_buffer[position + idx][: width // 2]

                    try:
                        stdscr.addstr(y, x, text_to_display)
                    except Exception:
                        stdscr.addstr(
                            y, x, "The line is longer than the terminal width"
                        )

            key: cython.int = stdscr.getch()
            if key == ord("k"):
                position -= 1
            elif key == ord("j"):
                position += 1
            elif key == ord("\n"):
                position += screen_height
            elif key == ord("/"):
                stdscr.clear()
                curses.echo()  # Включаем эхо-ввод текста
                stdscr.addstr(0, 0, "Enter line number: ")
                input_str = stdscr.getstr().decode("utf-8")
                try:
                    new_pos: cython.long = int(input_str.strip()) - 1
                    if new_pos >= 0:
                        position = new_pos
                        buffer_size = position
                except ValueError:
                    pass
                finally:
                    curses.noecho()
            elif key == ord("q"):
                break

    curses.wrapper(show_file)
