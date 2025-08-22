# MatrixTableConsumer v1.2.0

To install this package run (you need to have Go):

```bash
pip install build
pip install .
```

To compile Go modules with C types to work with Python run:

```bash
export CGO_ENABLED=1

go build -o functions.so -buildmode=c-shared functions/functions.go
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

To run tests, use:

```bash
pytest
```

> You may donate to [matrix_table_consumer](https://github.com/PHILIPP111007/matrix_table_consumer) project:
>
> * Ethereum: 0xE2e2D675a3843f4ED211BB93847ad18b0A6fe7c6
