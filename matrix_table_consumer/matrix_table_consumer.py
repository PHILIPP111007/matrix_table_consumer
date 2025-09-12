__version__ = "1.2.9"


import os
import sys
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
from bio2zarr import vcf as vcf2zarr
import zarr
from zarr.core import Array
from zarr.hierarchy import Group
import pandas as pd

from .functions_py.logger import logger_error, logger_info

try:
    from .functions_py import convert_rows_to_hail, qc_analysis, gwas
except ImportError:
    logger_error("No module named convert_rows_to_hail and sample_qc_analysis")


NUM_CPU = multiprocessing.cpu_count()

Content: TypeAlias = dict
Rows: TypeAlias = list[dict]

current_dir = os.path.dirname(__file__)
library_path = os.path.join(current_dir, "main.so")

lib = ctypes.CDLL(library_path)
CollectAll = lib.CollectAll
Collect = lib.Collect
Count = lib.Count

CollectAll.argtypes = [ctypes.c_char_p, ctypes.c_int]
CollectAll.restype = ctypes.c_char_p

Collect.argtypes = [
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_int,
]
Collect.restype = ctypes.c_char_p

Count.argtypes = [ctypes.c_char_p]
Count.restype = ctypes.c_int


def string_to_binary(string: str) -> bytes:
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


def get_json(path: str) -> Content:
    if not os.path.exists(path):
        logger_error("File not found")
        sys.exit(1)

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


def load_dill(path: str) -> Content:
    if not os.path.exists(path):
        logger_error("File not found")
        sys.exit(1)

    with open(path, "rb") as file:
        content = dill.load(file, recurse=True)
        return content


class MatrixTableConsumer:
    def __init__(self, vcf_path: str, reference_genome: str = "GRCh38") -> None:
        self.visited_objects = set()
        self.visited_objects_values = {}
        self.visited_objects_values_for_restoring = []
        self.start_row = 1
        self.vcf_path = vcf_path
        self.reference_genome = reference_genome

        if not os.path.exists(self.vcf_path):
            logger_error("Input vcf not found")

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
        """Saves matrix table metadata to json format"""

        progress_bar = tqdm(total=2, desc="Extracting fields")

        content = self._extract_fields(obj=mt)
        progress_bar.update(1)

        save_json(path=json_path, content=content)
        progress_bar.update(1)
        progress_bar.close()
        return content

    def prepare_metadata_for_loading(self, json_path: str) -> hl.MatrixTable:
        """Loads table metadata"""

        progress_bar = tqdm(total=3, desc="Prepare metadata for loading")
        content = get_json(path=json_path)
        progress_bar.update(1)

        content = self._compress_fields(obj=content)
        progress_bar.update(1)

        mt = hl.MatrixTable(mir=content["_mir"])
        mt.__dict__.update(content)
        progress_bar.update(1)
        return mt

    def collect(self, num_rows: int, num_cpu: int = 1) -> Rows:
        """Gives `num_rows` rows from vcf file (it can also open vcf.gz)"""

        if not os.path.exists(self.vcf_path):
            logger_error("File not found")
            sys.exit(1)

        vcf_path_encoded = self.vcf_path.encode("utf-8")
        s = Collect(num_rows, self.start_row, vcf_path_encoded, num_cpu)
        s = s.decode("utf-8")
        rows = json.loads(s)
        self.start_row += len(rows)

        return rows

    def collect_all(self, num_cpu: int = 1) -> Rows:
        """Collects all table rows from vcf file (it can also open vcf.gz)"""

        if not os.path.exists(self.vcf_path):
            logger_error("File not found")
            sys.exit(1)
        logger_info("Collecting data")

        vcf_path_encoded = self.vcf_path.encode("utf-8")
        s = CollectAll(vcf_path_encoded, num_cpu)
        s = s.decode("utf-8")
        rows = json.loads(s)

        logger_info("End")
        return rows

    def count(self) -> int:
        vcf_path_encoded = self.vcf_path.encode("utf-8")
        c = Count(vcf_path_encoded)
        return c

    def convert_rows_to_hail(self, rows: Rows) -> list[hl.Struct]:
        """Converts rows to Matrix Table format"""

        structs = convert_rows_to_hail.convert_rows_to_hail_c(
            rows=rows, reference_genome=self.reference_genome
        )
        return structs

    def create_hail_table(self, rows: Rows) -> hl.Table:
        """Collects table from rows"""

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

    def save_vcf_as_zarr(
        self,
        output_vcz: str,
        num_cpu: int = 1,
        show_progress: bool = False,
    ) -> None:
        """Save VCF in zarr format (.vcz)"""

        vcf2zarr.convert(
            vcfs=[self.vcf_path],
            vcz_path=output_vcz,
            worker_processes=num_cpu,
            show_progress=show_progress,
        )

    def load_zarr_data(self, vcz_path: str) -> Array | Group:
        """Loads zarr data"""

        if not os.path.exists(vcz_path):
            logger_error("Input vcz not found")

        data = zarr.open(vcz_path, mode="r")
        return data

    def sample_qc_analysis(self, zarr_data: Array | Group, num_cpu: int = 1) -> pd.DataFrame:
        """Sample quality analysis"""

        df: pd.DataFrame = qc_analysis.qc_analysis_c(zarr_data=zarr_data, num_cpu=num_cpu)
        return df


    def run_gwas(self, zarr_data: Array | Group, phenotypes, covariates=None, chunk_size: int = 5000) -> pd.DataFrame:
        """Sample quality analysis"""

        df: pd.DataFrame = gwas.run_gwas(zarr_data=zarr_data, phenotypes=phenotypes, covariates=covariates, chunk_size=chunk_size)

        print(
            f"Of {len(df)} significant variants found (p < 0.05): {np.sum(df['p_value'] < 0.05)}"
        )

        return df


if __name__ == "__main__":
    print(f"MatrixTableConsumer v{__version__}.\n")
