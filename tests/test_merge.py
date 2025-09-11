import os

from ..matrix_table_consumer import vcf_tools


def test_merge() -> None:
    vcf1 = "./data/merge/test1.vcf"
    vcf2 = "./data/merge/test2.vcf"
    output_vcf = "./data/merge/test_output_merged.vcf"
    output_test_vcf = "./data/merge/test_merged.vcf"
    file_with_vcfs = "./data/merge/vcfs.txt"

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

    vcf_tools.merge(
        output_vcf=output_vcf,
        file_with_vcfs=file_with_vcfs,
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
