import multiprocessing

import pandas as pd
import numpy as np
cimport numpy as np

import cython
from zarr.core import Array
from zarr.hierarchy import Group
from tqdm import tqdm


def calculate_sample_metrics(args: tuple) -> dict[str, float]:
    cdef:
        long variant_idx
        double call_rate, heterozygosity, missing_rate
        double mean_depth, median_depth, depth_sd
        double het_hom_ratio, ts_tv_ratio, f_stat
        long n_diploid
        long hom_ref, hom_alt, het

    sample_idx: cython.long
    genotypes: Group
    ploidy: cython.long
    variant_alleles: Group
    sample_names: np.ndarray[str]
    n_variants: cython.long
    progress_bar: tqdm

    sample_idx, genotypes, ploidy, variant_alleles, sample_names, n_variants = args

    progress_bar = tqdm(total=1, desc=f"Processing {sample_names[sample_idx]}", leave=True)

    sample_data = genotypes[:, sample_idx, :][:]  # Явная загрузка в память

    missing_mask = (sample_data == -1)
    missing_per_variant = np.any(missing_mask, axis=1)
    call_rate = 1 - np.mean(missing_per_variant)

    # вычисление глубины
    depth_per_variant = np.sum(sample_data >= 0, axis=1, dtype=float) / float(ploidy)
    non_zero_depth = depth_per_variant[depth_per_variant > 0]
    if len(non_zero_depth) > 0:
        mean_depth = np.mean(non_zero_depth)
        median_depth = np.median(non_zero_depth)
        depth_sd = np.std(non_zero_depth)
    else:
        mean_depth = 0.0
        median_depth = 0.0
        depth_sd = 0.0

    # Гетерозиготность и гомозиготность
    valid_mask = ~missing_per_variant
    valid_data = sample_data[valid_mask][:]

    hom_ref = 0
    hom_alt = 0
    het = 0
    ts_count = 0
    tv_count = 0
    singletons = 0
    private_variants = 0

    len_valid_data: cython.long = len(valid_data)
    if len_valid_data > 0:
        for variant_idx in range(len_valid_data):
            gt = valid_data[variant_idx][:]

            # Подсчет гом/гет
            if ploidy == 2:
                if gt[0] == gt[1]:
                    if gt[0] == 0:
                        hom_ref += 1
                    else:
                        hom_alt += 1
                else:
                    het += 1

            # Ts/Tv ratio (требует информации о вариантах)
            if variant_alleles is not None and len(gt) == 2:
                if gt[0] != gt[1]:  # Гетерозигота
                    alt_idx1 = gt[0] - 1 if gt[0] > 0 else -1
                    alt_idx2 = gt[1] - 1 if gt[1] > 0 else -1

                    if alt_idx1 >= 0 and alt_idx2 >= 0:
                        # Ts/Tv ratio (требует информации о вариантах)
                        if variant_alleles is not None and len(gt) == 2 and gt[0] != gt[1]:  # Только для гетерозигот
                            ref_allele = variant_alleles[variant_idx][0]
                            alt_alleles = variant_alleles[variant_idx][1:]

                            # Получаем конкретные аллели для этого генотипа
                            alleles_in_gt = []
                            for allele_idx in gt:
                                if allele_idx == 0:
                                    alleles_in_gt.append(ref_allele)
                                elif allele_idx > 0 and allele_idx - 1 < len(alt_alleles):
                                    alleles_in_gt.append(alt_alleles[allele_idx - 1])
                                else:
                                    alleles_in_gt.append(None)

                            # Убеждаемся, что оба аллеля определены
                            if None not in alleles_in_gt and len(set(alleles_in_gt)) == 2:
                                # Сортируем аллели
                                allele_pair = tuple(sorted(alleles_in_gt))

                                # Проверяем является ли это транзицией
                                if allele_pair in {('A', 'G'), ('C', 'T')}:
                                    ts_count += 1
                                else:
                                    tv_count += 1

        # Расчет метрик
        heterozygosity = float(het) / float(len_valid_data)

        total_hom = hom_ref + hom_alt
        het_hom_ratio = float(het) / float(total_hom) if total_hom > 0 else 0.0

        # Ts/Tv ratio
        ts_tv_ratio = float(ts_count) / float(tv_count) if tv_count > 0 else 0.0

        # Коэффициент инбридинга
        expected_het = 2.0 * heterozygosity * (1.0 - heterozygosity)
        f_stat = 1 - (float(het) / (expected_het * float(len_valid_data))) if expected_het > 0.0 else 0.0

    else:
        heterozygosity = 0.0
        het_hom_ratio = 0.0
        ts_tv_ratio = 0.0
        f_stat = 0.0

    missing_rate = 1.0 - call_rate

    n_diploid = np.sum(np.all(sample_data >= 0, axis=1))
    percent_diploid = float(n_diploid) / float(n_variants) if n_variants > 0 else 0.0

    progress_bar.update(1)
    progress_bar.close()

    return {
        "sample_id": sample_names[sample_idx],
        "call_rate": call_rate,
        "heterozygosity": heterozygosity,
        "missing_rate": missing_rate,
        "mean_depth": mean_depth,
        "median_depth": median_depth,
        "depth_sd": depth_sd,
        "het_hom_ratio": het_hom_ratio,
        "n_singletons": singletons,
        "n_private_variants": private_variants,
        "transition_transversion_ratio": ts_tv_ratio,
        "inbreeding_coefficient": f_stat,
        "percent_diploid": percent_diploid
    }




@cython.boundscheck(False)
@cython.wraparound(False)
@cython.initializedcheck(False)
@cython.cdivision(True)
@cython.infer_types(True)
def qc_analysis_c(zarr_data: Group) -> pd.DataFrame:
    """Sample quality analysis"""

    genotypes: Array = zarr_data["call_genotype"]
    sample_names: np.ndarray[str] = zarr_data["sample_id"][:]

    try:
        variant_alleles: Group = zarr_data["variant_allele"][:]
    except KeyError:
        variant_alleles = None

    n_variants: cython.long
    n_samples: cython.long
    ploidy: cython.long
    n_variants, n_samples, ploidy = genotypes.shape

    qc_metrics: dict[str, list] = {
        "sample_id": [],
        "call_rate": [],
        "heterozygosity": [],
        "missing_rate": [],
        "mean_depth": [],
        "median_depth": [],
        "depth_sd": [],
        "het_hom_ratio": [],
        "n_singletons": [],
        "n_private_variants": [],
        "transition_transversion_ratio": [],
        "inbreeding_coefficient": [],
        "percent_diploid": [],
    }


    # Prepare arguments for each sample
    args_list = []
    for sample_idx in range(10):
        args_list.append((sample_idx, genotypes, ploidy, variant_alleles, sample_names, n_variants))

    # Use multiprocessing Pool
    with multiprocessing.Pool(processes=3) as pool:
        results = pool.map(calculate_sample_metrics, args_list)

    # Collect results
    for result in results:
        for key in qc_metrics.keys():
            qc_metrics[key].append(result[key])
    
    return pd.DataFrame(qc_metrics)
