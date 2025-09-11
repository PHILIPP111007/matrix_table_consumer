import os

from ..matrix_table_consumer import vcf_tools


def test_sort() -> None:
    vcf = "./data/sort/test.vcf"
    output_vcf = "./data/sort/test_output_sorted.vcf"
    output_test_vcf = "./data/sort/test_sorted.vcf"

    vcf_tools.sort(
        vcf_path=vcf,
        output_vcf=output_vcf,
        chunk_size=100,
    )

    with (
        open(output_test_vcf, "r") as output_test_file,
        open(output_vcf, "r") as output_file,
    ):
        output_test_file_text = output_test_file.read()
        output_file_text = output_file.read()

        assert output_test_file_text == output_file_text

    os.remove(output_vcf)

    vcf_tools.sort(
        vcf_path=vcf,
        output_vcf=output_vcf,
        chunk_size=1,
    )

    with (
        open(output_test_vcf, "r") as output_test_file,
        open(output_vcf, "r") as output_file,
    ):
        output_test_file_text = output_test_file.read()
        output_file_text = output_file.read()

        assert output_test_file_text == output_file_text

    os.remove(output_vcf)
