# MatrixTableConsumer v1.2.7

To install this package run (you need to have Go):

```bash
pip install build
pip install .
```

To compile Go modules with C types to work with Python run:

```bash
export CGO_ENABLED=1

go build -o main.so -buildmode=c-shared main.go
```

We have a class `MatrixTableConsumer`, which performs operations on Hail matrix table:

- `MatrixTableConsumer().prepare_metadata_for_saving` saves matrix table metadata to json format

- `MatrixTableConsumer().prepare_metadata_for_loading` loads table metadata

- `MatrixTableConsumer().collect` gives `num_rows` rows from vcf file (it can also open vcf.gz)

- `MatrixTableConsumer().collect_all` collects all table rows from vcf file (it can also open vcf.gz)

- `MatrixTableConsumer().convert_rows_to_hail` converts rows to Matrix Table format

- `MatrixTableConsumer().create_hail_table` collects table from rows

- `MatrixTableConsumer().combine_hail_matrix_table_and_table` combines MatrixTable and Table

- `MatrixTableConsumer().count` returns number of rows in the vcf file

___

You can look at the `main.ipynb` file, which contains examples of using `MatrixTableConsumer`

You can look at the `benchmarks.md` file, which contains benchmark of my program and bcftools

You can filter `.vcf` and `.vcf.gz` files (`&&` and `||` operators is available):

```bash
vcf_tools -filter \
    -o ./data/test_1.vcf \
    -vcf ./data/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz \
    -i "(QUAL>=90 && AF>=0.00001) || AF>=0.001" \
    -num_cpu 7
```

You can merge `.vcf` files:

```bash
vcf_tools -merge \
    -vcf ./data/test1.vcf \
    -vcf2 ./data/test2.vcf \
    -o ./data/test_merged.vcf
```

To run tests, use:

```bash
pytest
```
