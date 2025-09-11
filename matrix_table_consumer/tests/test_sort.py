import os

from matrix_table_consumer import vcf_tools


def test_merge() -> None:
    vcf1 = "../matrix_table_consumer/data/merge/test1.vcf"
    vcf2 = "../matrix_table_consumer/data/merge/test2.vcf"
    output_vcf = "../matrix_table_consumer/data/merge/test_output_merged.vcf"
    output_test_vcf = "../matrix_table_consumer/data/merge/test_merged.vcf"

    vcf_tools.merge(
        vcf1=vcf1,
        vcf2=vcf2,
        output_vcf=output_vcf,
    )

    with (
        open(output_test_vcf, "r") as output_test_file,
        open(output_vcf, "r") as output_file,
    ):
        output_test_file_set = set()
        output_file_set = set()

        for line in output_test_file:
            if line.startswith("#"):
                continue
            output_test_file_set.add(line)

        for line in output_file:
            if line.startswith("#"):
                continue
            output_file_set.add(line)

        assert output_test_file_set == output_file_set
    os.remove(output_vcf)
