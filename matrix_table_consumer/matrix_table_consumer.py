__version__ = "1.0.0"


import os
import ctypes
import shutil
import multiprocessing
from datetime import datetime
import gzip
import dill
import json
from typing import TypeAlias
from tqdm import tqdm

import hail as hl


NUM_CPU = multiprocessing.cpu_count()


Content: TypeAlias = dict
Rows: TypeAlias = list[dict]

current_dir = os.path.dirname(__file__)
library_path = os.path.join(current_dir, "functions.so")

lib = ctypes.CDLL(library_path)
CollectAll = lib.CollectAll
Collect = lib.Collect
Count = lib.Count

CollectAll.argtypes = [ctypes.c_char_p, ctypes.c_bool, ctypes.c_int]
CollectAll.restype = ctypes.c_char_p

Collect.argtypes = [
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_bool,
    ctypes.c_int,
]
Collect.restype = ctypes.c_char_p

Count.argtypes = [ctypes.c_char_p, ctypes.c_bool]
Count.restype = ctypes.c_int


def get_time() -> str:
    return datetime.now().strftime("%d-%m-%Y %H:%M:%S")


def logger_info(s: str) -> None:
    t = get_time()
    print(f"[{t}] - INFO - {s}")


def logger_error(s: str) -> None:
    t = get_time()
    print(f"[{t}] - ERROR - {s}")


def string_to_binary(string: str):
    """Encode the string to bytes using UTF-8 encoding"""

    encoded_bytes = string.encode("utf-8")
    return encoded_bytes


def save_as_gzip(path: str, content: str) -> None:
    encoded_bytes = string_to_binary(string=content)
    with gzip.open(path, "wb") as file:
        file.write(encoded_bytes)


def save_json(path: str, content: Content) -> None:
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)

    with open(path, "w") as file:
        json.dump(content, file, indent=4)


def get_json(path: str) -> Content | None:
    if not os.path.exists(path):
        logger_error("File not found")
        exit(1)

    with open(path, "r") as file:
        content = json.load(file)
        return content


def save_as_dill(path: str, content: Content) -> None:
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)

    with open(path, "wb") as file:
        dill.dump(content, file)


def load_dill(path: str) -> Content | None:
    if not os.path.exists(path):
        logger_error("File not found")
        exit(1)

    with open(path, "rb") as file:
        content = dill.load(file, recurse=True)
        return content


class MatrixTableConsumer:
    def __init__(
        self, vcf_path: str, is_gzip: bool = False, reference_genome: str = "GRCh38"
    ) -> None:
        self.visited_objects = set()
        self.visited_objects_values = {}
        self.visited_objects_values_for_restoring = []
        self.start_row = 0
        self.vcf_path = vcf_path
        self.is_gzip = is_gzip
        self.reference_genome = reference_genome

    def _extract_fields(self, obj) -> Content:
        """Returns JSON with uncompressed object classes"""

        result = {}  # Новый словарь для сбора результатов
        if id(obj) in self.visited_objects:
            return self.visited_objects_values.get(id(obj))
        self.visited_objects.add(id(obj))

        if hasattr(obj, "__dict__"):
            for key, value in obj.__dict__.items():
                items_for_restoring = []
                items_for_restoring.append(key)
                items_for_restoring.append(value)
                if hasattr(key, "__dict__"):
                    key = str(key)
                value = self._extract_fields(value)
                items_for_restoring.append(value)
                self.visited_objects_values_for_restoring.append(items_for_restoring)
                result[key] = value
                self.visited_objects_values[id(value)] = value
        elif isinstance(obj, dict):
            for key, value in obj.items():
                if hasattr(key, "__dict__"):
                    key = str(key)
                value = self._extract_fields(value)
                result[key] = value
                self.visited_objects_values[id(value)] = value
        elif isinstance(obj, list) or isinstance(obj, tuple):
            lst = []
            for value in obj:
                value = self._extract_fields(value)
                lst.append(value)
            self.visited_objects_values[id(lst)] = lst
            return lst
        elif isinstance(obj, set) and not obj:
            value = {}
            self.visited_objects_values[id(value)] = value
            return value
        elif isinstance(obj, set):
            new_set = []
            for value in obj:
                if hasattr(value, "__dict__"):
                    value = self._extract_fields(value)
                new_set.append(value)
                self.visited_objects_values[id(value)] = value
            return new_set
        elif isinstance(obj, type(datetime.year)):
            value = str(obj)
            value = value.replace("'", '\\"')
            self.visited_objects_values[id(value)] = value
            return value
        elif isinstance(obj, str):
            value = obj.replace("'", '\\"')
            self.visited_objects_values[id(value)] = value
            return value
        elif isinstance(obj, hl.expr.matrix_type.tmatrix):
            value = str(obj)
            self.visited_objects_values[id(value)] = value
            return value
        else:
            self.visited_objects_values[id(obj)] = obj
            return obj
        return result

    def _compress_fields(self, obj: Content) -> Content:
        """Returns JSON with restored object classes"""

        result = {}
        if isinstance(obj, dict):
            for key, value in obj.items():
                for item in self.visited_objects_values_for_restoring:
                    if str(item[0]) == key and item[2] == value:
                        result[key] = item[1]
        return result

    def prepare_metadata_for_saving(
        self, json_path: str, mt: hl.MatrixTable
    ) -> Content:
        logger_info("Extracting fields")
        content = self._extract_fields(obj=mt)

        logger_info("Save json")
        save_json(path=json_path, content=content)
        logger_info("End")
        return content

    def prepare_metadata_for_loading(self, json_path: str) -> hl.MatrixTable:
        logger_info("Prepare metadata for loading")
        content = get_json(path=json_path)

        logger_info("Compressing fields")
        content = self._compress_fields(obj=content)

        logger_info("Creating matrix table")
        mt = hl.MatrixTable(mir=content["_mir"])
        mt.__dict__.update(content)
        logger_info("End")
        return mt

    def collect(self, num_rows: int, num_cpu: int = 1) -> Rows:
        if not os.path.exists(self.vcf_path):
            logger_error("File not found")
            exit(1)
        logger_info("Collecting data")

        vcf_path_encoded = self.vcf_path.encode("utf-8")
        s = Collect(num_rows, self.start_row, vcf_path_encoded, self.is_gzip, num_cpu)
        s = s.decode("utf-8")
        rows = json.loads(s)
        self.start_row += len(rows)

        logger_info("End")
        return rows

    def collect_all(self, num_cpu: int = 1) -> Rows:
        if not os.path.exists(self.vcf_path):
            logger_error("File not found")
            exit(1)
        logger_info("Collecting data")

        vcf_path_encoded = self.vcf_path.encode("utf-8")
        s = CollectAll(vcf_path_encoded, self.is_gzip, num_cpu)
        s = s.decode("utf-8")
        rows = json.loads(s)

        logger_info("End")
        return rows

    def count(self) -> int:
        vcf_path_encoded = self.vcf_path.encode("utf-8")
        c = Count(vcf_path_encoded, self.is_gzip)
        return c

    def convert_rows_to_hail(self, rows: Rows) -> Rows:
        structs = []
        for row in tqdm(rows, desc="Converting rows to hail"):
            row_fields = {}

            locus = hl.Locus(
                contig=row["CHROM"],
                position=row["POS"],
                reference_genome=self.reference_genome,
            )
            alleles = [row["REF"], row["ALT"]]
            rsid = row["ID"]
            qual = row["QUAL"]
            filters = row["FILTER"] if row["FILTER"] != "." else None
            info = {"info": row["INFO"]}
            info = hl.Struct(**info)
            entries = []

            row_fields["locus"] = locus
            row_fields["alleles"] = alleles
            row_fields["rsid"] = rsid
            row_fields["qual"] = qual
            row_fields["filters"] = filters
            row_fields["info"] = info
            row_fields["entries"] = entries

            struct = hl.Struct(**row_fields)
            structs.append(struct)
        return structs

    def create_hail_table(self, rows: Rows) -> hl.Table:
        row_schema = hl.tstruct(
            locus=hl.tlocus(reference_genome=self.reference_genome),
            alleles=hl.tarray(hl.tstr),
            rsid=hl.tstr,
            qual=hl.tint,
            filters=hl.tstr,
            info=hl.tstruct(info=hl.tstr),
            entries=hl.tarray(hl.tstruct()),
        )
        t = hl.Table.parallelize([], schema=row_schema)
        t = t.union(hl.Table.parallelize(rows, schema=row_schema))
        t = t.key_by("locus")
        return t

    def combine_hail_matrix_table_and_table(
        self, mt: hl.MatrixTable, table: hl.Table
    ) -> hl.MatrixTable:
        mt = mt.annotate_rows(new_field=table[mt.locus])
        return mt


if __name__ == "__main__":
    print(f"MatrixTableConsumer v{__version__}.")
