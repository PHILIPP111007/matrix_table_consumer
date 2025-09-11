from ..matrix_table_consumer.matrix_table_consumer import MatrixTableConsumer


def test_count() -> None:
    vcf_path = "./data/count/test1.vcf"
    consumer = MatrixTableConsumer(vcf_path=vcf_path)

    count = consumer.count()
    assert count == 13, count


if __name__ == "__main__":
    test_count()
