import os

from matrix_table_consumer.vcf_tools import VCFTools


def test_merge() -> None:
    vcf1 = "../matrix_table_consumer/data/test1.vcf"
    vcf2 = "../matrix_table_consumer/data/test2.vcf"
    output_vcf = "../matrix_table_consumer/data/test_merged_2.vcf"
    output_test_vcf = "../matrix_table_consumer/data/test_merged.vcf"

    vcftools = VCFTools()

    vcftools.merge(
        vcf1=vcf1,
        vcf2=vcf2,
        output_vcf=output_vcf,
        is_gzip=False,
        is_gzip2=False,
    )

    with (
        open(output_test_vcf, "r") as output_test_file,
        open(output_vcf, "r") as output_file,
    ):
        file1 = output_test_file.read()
        file2 = output_file.read()

        assert file1 == file2

    os.remove(output_vcf)
