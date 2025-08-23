from matrix_table_consumer import MatrixTableConsumer


test_text = [
    {
        "QUAL": 1,
        "POS": 13100,
        "CHROM": "chr1",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 2,
        "POS": 25734793,
        "CHROM": "chr1",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 3,
        "POS": 37323930,
        "CHROM": "chr1",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 4,
        "POS": 1234,
        "CHROM": "chr2",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 5,
        "POS": 1235,
        "CHROM": "chr2",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 6,
        "POS": 1234,
        "CHROM": "chr3",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 7,
        "POS": 1234,
        "CHROM": "chr4",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 8,
        "POS": 1235,
        "CHROM": "chr4",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 9,
        "POS": 1236,
        "CHROM": "chr4",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 10,
        "POS": 1234,
        "CHROM": "chr5",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 11,
        "POS": 1234,
        "CHROM": "chr6",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 12,
        "POS": 1235,
        "CHROM": "chr6",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
    {
        "QUAL": 13,
        "POS": 1236,
        "CHROM": "chr6",
        "ID": ".",
        "REF": "C",
        "ALT": "T",
        "FILTER": "PASS",
        "INFO": ".",
    },
]


def test_collect_all() -> None:
    vcf_path = "../matrix_table_consumer/data/test1.vcf"

    consumer = MatrixTableConsumer(
        vcf_path=vcf_path, is_gzip=False, reference_genome="GRCh37"
    )

    rows = consumer.collect_all(num_cpu=1)
    assert rows == test_text, rows
