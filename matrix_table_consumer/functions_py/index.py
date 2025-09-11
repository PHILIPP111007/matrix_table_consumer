import pysam


def index_vcf(vcf_path: str):
    try:
        pysam.tabix_index(vcf_path, preset="vcf", force=True, keep_original=True)
    except Exception as e:
        print(f"Error: {e}")
