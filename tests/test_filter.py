import os

from matrix_table_consumer import vcf_tools


def test_filter() -> None:
    vcf = "./data/filter/test3.vcf"
    output_vcf = "./data/filter/test_filtered_output.vcf"
    output_test_vcf_1 = "./data/filter/test_filtered_1.vcf"
    output_test_vcf_2 = "../matrix_table_consumer/data/filter/test_filtered_2.vcf"
    output_test_vcf_3 = "./data/filter/test_filtered_3.vcf"

    vcf_tools.filter(
        include="FILTER=='PASS'",
        input_vcf=vcf,
        output_vcf=output_vcf,
        num_cpu=1,
    )

    with (
        open(output_test_vcf_1, "r") as output_test_file,
        open(output_vcf, "r") as output_file,
    ):
        file1 = output_test_file.read()
        file2 = output_file.read()

        assert file1 == file2
    os.remove(output_vcf)

    vcf_tools.filter(
        include="AF>=0.03",
        input_vcf=vcf,
        output_vcf=output_vcf,
        num_cpu=1,
    )

    with (
        open(output_test_vcf_2, "r") as output_test_file,
        open(output_vcf, "r") as output_file,
    ):
        file1 = output_test_file.read()
        file2 = output_file.read()

        assert file1 == file2
    os.remove(output_vcf)

    vcf_tools.filter(
        include="(AF>=0.03 || AC>=2)",
        input_vcf=vcf,
        output_vcf=output_vcf,
        num_cpu=1,
    )

    with (
        open(output_test_vcf_3, "r") as output_test_file,
        open(output_vcf, "r") as output_file,
    ):
        file1 = output_test_file.read()
        file2 = output_file.read()

        assert file1 == file2
    os.remove(output_vcf)


if __name__ == "__main__":
    test_filter()
