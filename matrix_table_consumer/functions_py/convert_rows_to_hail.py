import cython
from tqdm import tqdm
import hail as hl


def convert_rows_to_hail_c(rows: list[dict], reference_genome: str) -> list[hl.Struct]:
    """
    Converting strings from VCF format to Hail structure objects.
    """

    i: cython.long
    len_rows: cython.long = len(rows)
    structs: list = [0] * len_rows
    progress_bar = tqdm(total=len_rows, desc="Converting rows to hail")

    for i in range(len_rows):
        row: dict = rows[i]

        chrom: str = row["CHROM"]
        pos: cython.int = row["POS"]
        locus: hl.Locus = hl.Locus(
            contig=chrom, position=pos, reference_genome=reference_genome
        )

        ref: str = row["REF"]
        alt: str = row["ALT"]
        alleles: list = [ref, alt]

        rsid: str = row["ID"]
        qual: cython.float = row["QUAL"]
        filters: str = row["FILTER"]

        info_dict: dict = {"info": row["INFO"]}
        info_struct: hl.Struct = hl.Struct(**info_dict)

        entries: list = []

        row_fields: dict = {
            "locus": locus,
            "alleles": alleles,
            "rsid": rsid,
            "qual": qual,
            "filters": filters,
            "info": info_struct,
            "entries": entries,
        }

        struct: hl.Struct = hl.Struct(**row_fields)
        structs[i] = struct

        progress_bar.update(1)

    progress_bar.close()

    return structs
