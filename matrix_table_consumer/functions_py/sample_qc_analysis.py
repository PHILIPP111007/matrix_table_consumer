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
@cython.cfunc
def sample_qc_analysis_c(zarr_data: Group):
    """Sample quality analysis"""

    genotypes: Array = zarr_data["call_genotype"]

    n_variants: cython.long
    n_samples: cython.long
    ploidy: cython.long

    n_variants, n_samples, ploidy = genotypes.shape
    print(f"Total samples: {n_samples}")

    qc_metrics: dict[str, list[cython.long | cython.double]] = {
        "sample_id": [],
        "call_rate": [],
        "heterozygosity": [],
        "missing_rate": [],
    }

    progress_bar_1 = tqdm(
        total=1, desc="Calculating QC metrics", position=0, leave=True
    )
    sample_idx: cython.long
    for sample_idx in range(1):
        sample_genotypes: Array = genotypes[:, sample_idx, :]

        # Расчет call rate
        missing_mask = sample_genotypes == -1  # Пропущенные генотипы
        call_rate: cython.double = 1 - np.mean(np.any(missing_mask, axis=1))

        # Расчет гетерозиготности
        het_count: cython.long = 0
        total_calls: cython.long = 0

        progress_bar_2 = tqdm(
            total=n_variants,
            desc="Handling variants",
            position=1,
            leave=False,
        )
        variant_idx: cython.long
        for variant_idx in range(n_variants):
            gt = sample_genotypes[variant_idx]
            if not np.any(gt == -1):  # Пропуски не учитываем
                total_calls += 1
                if gt[0] != gt[1]:
                    het_count += 1
            progress_bar_2.update(1)
        progress_bar_2.close()

        heterozygosity: cython.double = (
            het_count / total_calls if total_calls > 0 else 0.0
        )
        missing_rate: cython.double = 1 - call_rate

        qc_metrics["sample_id"].append(sample_idx)
        qc_metrics["call_rate"].append(call_rate)
        qc_metrics["heterozygosity"].append(heterozygosity)
        qc_metrics["missing_rate"].append(missing_rate)

        progress_bar_1.update(1)
    progress_bar_1.close()

    return pd.DataFrame(qc_metrics)
