import gzip


def sort_vcf(input_vcf: str, output_vcf: str):
    def chromosome_key(chrom: str) -> tuple[int, int]:
        chrom = chrom.upper()
        if chrom.startswith("CHR"):
            chrom = chrom[3:]

        try:
            return (0, int(chrom))
        except ValueError:
            special_chroms = {"X": 100, "Y": 101, "MT": 102, "M": 102}
            return (1, special_chroms.get(chrom, 1000 + hash(chrom)))

    try:
        records = []
        header_lines = []

        open_func = gzip.open if input_vcf.endswith(".gz") else open
        open_mode = "rt" if input_vcf.endswith(".gz") else "r"

        with open_func(input_vcf, open_mode) as f:
            for line in f:
                if line.startswith("#"):
                    header_lines.append(line)
                else:
                    parts = line.strip().split("\t")
                    if len(parts) >= 2:
                        chrom, pos = parts[0], int(parts[1])
                        records.append((chrom, pos, line))

        records.sort(key=lambda x: (chromosome_key(x[0]), x[1]))

        with open(output_vcf, "w") as file:
            file.writelines(header_lines)

            for chrom, pos, line in records:
                file.write(line)
    except Exception as e:
        print(f"Error: {e}")
