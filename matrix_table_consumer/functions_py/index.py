import pysam


def index_vcf(vcf_file):
    try:
        pysam.tabix_index(vcf_file, preset="vcf", force=True, keep_original=True)
    except Exception as e:
        print(f"Error: {e}")
