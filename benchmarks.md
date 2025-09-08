# Filter

VCF file downloaded from <https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/>

## bcftools

```bash
time bcftools filter \
    -o /home/phil/GitHub/matrix_table_consumer/data/test_1.vcf \
    /home/phil/GitHub/matrix_table_consumer/data/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz \
    -i "QUAL > 100"
```

time: 6:17.01m

```bash
time bcftools filter \
    -o /home/phil/GitHub/matrix_table_consumer/data/test_1.vcf \
    /home/phil/GitHub/matrix_table_consumer/data/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz \
    -i "QUAL > 100" \
    --threads 7
```

time: 6:15.39m (i dont know why but bcftools runs on only one CPU core)

## My version

```bash
time python matrix_table_consumer/vcf_tools.py -filter \
    -o /home/phil/GitHub/matrix_table_consumer/data/test_1.vcf \
    -vcf /home/phil/GitHub/matrix_table_consumer/data/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz \
    -i "QUAL > 100" \
    -num_cpu 1
```

time: 5:07.20m

```bash
time python matrix_table_consumer/vcf_tools.py -filter \
    -o /home/phil/GitHub/matrix_table_consumer/data/test_1.vcf \
    -vcf /home/phil/GitHub/matrix_table_consumer/data/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz \
    -i "QUAL > 100" \
    -num_cpu 7
```

time: 2:16.09m
