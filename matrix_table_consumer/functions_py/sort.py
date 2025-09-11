import sys
import gzip


def sort_vcf(input_vcf: str, output_vcf: str):
    """
    реализация сортировки VCF на чистом Python.
    Медленнее, но не требует установки bcftools.

    Параметры:
    input_vcf (str): Путь к входному VCF файлу.
    output_vcf (str): Путь к выходному файлу.

    Возвращает:
    """

    def chromosome_key(chrom: str) -> tuple[int, int]:
        """Функция для правильной сортировки хромосом."""
        # Преобразуем названия хромосом в числовой формат для сортировки
        chrom = chrom.upper()
        if chrom.startswith("CHR"):
            chrom = chrom[3:]

        try:
            # Числовые хромосомы
            return (0, int(chrom))
        except ValueError:
            # Специальные хромосомы (X, Y, MT и т.д.)
            special_chroms = {"X": 100, "Y": 101, "MT": 102, "M": 102}
            return (1, special_chroms.get(chrom, 1000 + hash(chrom)))

    try:
        # Читаем и парсим VCF файл
        records = []
        header_lines = []

        # Определяем, сжат ли файл
        open_func = gzip.open if input_vcf.endswith(".gz") else open
        open_mode = "rt" if input_vcf.endswith(".gz") else "r"

        with open_func(input_vcf, open_mode) as f:
            for line in f:
                if line.startswith("#"):
                    header_lines.append(line)
                else:
                    # Парсим данные варианта
                    parts = line.strip().split("\t")
                    if len(parts) >= 2:
                        chrom, pos = parts[0], int(parts[1])
                        records.append((chrom, pos, line))

        # Сортируем записи: сначала по хромосоме, затем по позиции
        records.sort(key=lambda x: (chromosome_key(x[0]), x[1]))

        # Записываем отсортированный файл
        with open(output_vcf, "w") as file:
            # Записываем заголовок
            file.writelines(header_lines)

            # Записываем отсортированные записи
            for chrom, pos, line in records:
                file.write(line)

        print(f"Файл успешно отсортирован: {output_vcf}")

    except Exception as e:
        print(f"Ошибка при сортировке: {e}", file=sys.stderr)
