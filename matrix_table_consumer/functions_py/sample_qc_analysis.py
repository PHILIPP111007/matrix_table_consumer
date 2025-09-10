import pandas as pd
import numpy as np

from tqdm import tqdm
import cython
from zarr.core import Array
from zarr.hierarchy import Group


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.initializedcheck(False)
@cython.cdivision(True)
@cython.infer_types(True)
def sample_qc_analysis_c(zarr_data: Group) -> pd.DataFrame:
    """Sample quality analysis"""

    genotypes: Array = zarr_data["call_genotype"]
    sample_names: np.ndarray[str] = zarr_data["sample_id"][:]

    n_variants: cython.long
    n_samples: cython.long
    ploidy: cython.long

    n_variants, n_samples, ploidy = genotypes.shape

    qc_metrics: dict[str, list[cython.long | cython.double | str]] = {
        "sample_id": [],
        "call_rate": [],
        "heterozygosity": [],
        "missing_rate": [],
    }

    progress_bar_1 = tqdm(
        total=2, desc="Calculating QC metrics", position=0, leave=True
    )
    sample_idx: cython.long
    for sample_idx in range(2):
        sample_genotypes: Array = genotypes[:, sample_idx, :]
        sample_data = sample_genotypes[:]  # Явная загрузка в память

        missing_mask = (sample_data == -1)
        missing_per_variant = np.any(missing_mask, axis=1)
        call_rate: cython.double = 1 - np.mean(missing_per_variant)

        # Расчет гетерозиготности
        valid_mask = ~missing_per_variant
        valid_data = sample_data[valid_mask]
        
        if len(valid_data) > 0:
            if valid_data.shape[1] == 2:
                heterozygosity = np.mean(valid_data[:, 0] != valid_data[:, 1])
            else:
                heterozygosity = np.mean([len(np.unique(gt)) > 1 for gt in valid_data])
        else:
            heterozygosity = 0.0

        missing_rate: cython.double = 1 - call_rate

        qc_metrics["sample_id"].append(sample_names[sample_idx])
        qc_metrics["call_rate"].append(call_rate)
        qc_metrics["heterozygosity"].append(heterozygosity)
        qc_metrics["missing_rate"].append(missing_rate)

        progress_bar_1.update(1)
    progress_bar_1.close()

    return pd.DataFrame(qc_metrics)
