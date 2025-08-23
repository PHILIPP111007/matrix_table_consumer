# Filter

```bash
time bcftools filter \
    -o /home/phil/GitHub/matrix_table_consumer/data/test_1.vcf \
    /home/phil/GitHub/matrix_table_consumer/data/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz \
    -i "QUAL > 100"
```

time: \
real    7m27.324s \
user    7m27.334s \
sys     0m0.503s

```bash
time python matrix_table_consumer/matrix_table_consumer.py \
    -filter -o /home/phil/GitHub/matrix_table_consumer/data/test_1.vcf \
    -vcf /home/phil/GitHub/matrix_table_consumer/data/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz \
    -i "QUAL > 100" \
    -gzip
```

time: \
real    4m6.311s \
user    4m16.073s \
sys     0m12.961s
