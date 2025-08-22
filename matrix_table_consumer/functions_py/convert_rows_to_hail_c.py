import cython
from tqdm import tqdm
import hail as hl


def convert_rows_to_hail_c(rows: list[dict], reference_genome: str):
    """
    Конвертация строк из формата VCF в объекты структур Hail.

    :param rows: Список объектов строк VCF.
    :param reference_genome: Геномная референсная база.
    :return: Список структур Hail.
    """
    i: cython.long
    len_rows: cython.long = len(rows)
    structs: list = []
    progress_bar = tqdm(total=len_rows, desc="Converting rows to hail")

    for i in range(len_rows):
        row: object = rows[i]

        # Создаем локус
        locus: hl.Locus = hl.Locus(
            contig=row["CHROM"], position=row["POS"], reference_genome=reference_genome
        )

        # Собираем аллели
        alleles: list = [row["REF"], row["ALT"]]

        # Идентификатор RSID
        rsid: str = row["ID"]

        # Качество QUAL
        qual: cython.float = row["QUAL"]

        # Фильтры FILTER
        filters: str = row["FILTER"]

        # Информация INFO
        info_dict: dict = {"info": row["INFO"]}
        info_struct: hl.Struct = hl.Struct(**info_dict)

        # Пустые записи Entries
        entries: list = []

        # Заполняем поля
        row_fields: dict = {
            "locus": locus,
            "alleles": alleles,
            "rsid": rsid,
            "qual": qual,
            "filters": filters,
            "info": info_struct,
            "entries": entries,
        }

        # Добавляем структуру в итоговый список
        struct: hl.Struct = hl.Struct(**row_fields)
        structs.append(struct)

        # Обновление прогресса с использованием tqdm
        progress_bar.update(1)

    progress_bar.close()

    return structs
