from matrix_table_consumer import MatrixTableConsumer


def test_count() -> None:
    vcf_path = "/home/phil/GitHub/matrix_table_consumer/data/test1.vcf"
    consumer = MatrixTableConsumer(vcf_path=vcf_path)

    count = consumer.count()
    assert count == 13, count
