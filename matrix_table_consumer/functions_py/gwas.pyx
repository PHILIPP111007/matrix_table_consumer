import multiprocessing

import numpy as np
cimport numpy as np

import pandas as pd
from scipy import stats
import warnings
from zarr.hierarchy import Group
import cython

from .logger import logger_info


counter = multiprocessing.Value('i', 0)


def process_chunk(genotypes: Group, n_variants: cython.long, n_samples: cython.long, variant_ids: Group, phenotypes: np.ndarray, chunk_size: cython.int, X_cov, X_cov_T_X_cov_inv, start: cython.long):
    end: cython.long
    end = min(start + chunk_size, n_variants)
    genotype_chunk: Group = genotypes[start:end]

    variant_idx: cython.long
    for variant_idx in range(genotype_chunk.shape[0]):
        # Получение генотипов для варианта
        gt: Group = genotype_chunk[variant_idx, :, :]

        # Кодирование генотипов (0,1,2) - сумма аллелей
        # Правильное кодирование: 0 = ref/ref, 1 = ref/alt, 2 = alt/alt
        # Учитываем пропущенные значения (-1)
        valid_gt_mask = (gt != -1).all(axis=1)
        dosage: np.ndarray = np.zeros(n_samples)

        # Для валидных генотипов считаем дозу альтернативного аллеля
        i: cython.long
        for i in range(n_samples):
            if valid_gt_mask[i]:
                # Считаем количество альтернативных аллелей (0, 1, 2)
                dosage[i] = np.sum(gt[i, :] > 0)
            else:
                # Для пропущенных значений используем среднее
                dosage[i] = np.nan

        # Замена пропущенных значений средним по варианту
        mean_dosage = np.nanmean(dosage)
        dosage = np.nan_to_num(dosage, nan=mean_dosage)

        # Нормализация дозы (центрирование)
        dosage_normalized = dosage - np.mean(dosage)

        # Пропускаем варианты с нулевой дисперсией
        if np.var(dosage_normalized) < 1e-10:
            continue

        # Линейная регрессия с использованием проекционной матрицы
        try:
            # Проекция фенотипов на пространство ковариат
            proj_y: np.ndarray = X_cov @ (X_cov_T_X_cov_inv @ (X_cov.T @ phenotypes))
            y_resid: np.ndarray = phenotypes - proj_y

            # Проекция генотипов на пространство ковариат
            proj_x: np.ndarray = X_cov @ (X_cov_T_X_cov_inv @ (X_cov.T @ dosage_normalized))
            x_resid: np.ndarray = dosage_normalized - proj_x

            beta: float = np.sum(x_resid * y_resid) / np.sum(x_resid**2)

            # Остатки и стандартная ошибка
            residuals: np.ndarray = y_resid - beta * x_resid
            mse: float = np.sum(residuals**2) / (len(phenotypes) - X_cov.shape[1] - 1)
            se: float = np.sqrt(mse / np.sum(x_resid**2))

            # t-статистика и p-value
            t_stat: float = beta / se
            df = len(phenotypes) - X_cov.shape[1] - 1
            p_value: float = 2 * (1 - stats.t.cdf(abs(t_stat), df))

            counter.value += end - start
            logger_info(f"Processed: {counter.value} / {n_variants}")
            return {
                    "variant_index": start + variant_idx,
                    "variant_id": variant_ids[start + variant_idx],
                    "beta": beta,
                    "se": se,
                    "p_value": p_value,
                    "t_stat": t_stat,
                    "maf": np.mean(dosage) / 2,  # Minor allele frequency
                }
            

        except np.linalg.LinAlgError:
            # Пропускаем варианты
            return {
                    "variant_index": start + variant_idx,
                    "variant_id": variant_ids[start + variant_idx],
                    "beta": None,
                    "se": None,
                    "p_value": None,
                    "t_stat": None,
                    "maf": None,
                }
        except Exception as e:
            warnings.warn(
                f"Error processing variant {start + variant_idx}: {e}"
            )
            return {
                    "variant_index": start + variant_idx,
                    "variant_id": variant_ids[start + variant_idx],
                    "beta": None,
                    "se": None,
                    "p_value": None,
                    "t_stat": None,
                    "maf": None,
                }


def run_gwas_c(zarr_data: Group, phenotypes: np.ndarray, covariates: np.ndarray = None, chunk_size: cython.int = 5000, num_cpu: int = 1):
    """Simplified GWAS using Zarr"""

    genotypes: Group = zarr_data["call_genotype"]
    variant_ids: Group = zarr_data["variant_id"]
    n_variants: cython.long = genotypes.shape[0]
    n_samples: cython.long = genotypes.shape[1]

    if len(phenotypes) != n_samples:
        raise ValueError(
            f"The dimension of phenotypes ({len(phenotypes)}) does not match the number of samples ({n_samples})"
        )

    if covariates is not None and len(covariates) != n_samples:
        raise ValueError(
            f"The dimension of covariates ({len(covariates)}) does not match the number of samples ({n_samples})"
        )

    gwas_results: list[dict[str, int | float]] = []

    # Подготовка ковариат (включая intercept)
    if covariates is not None:
        X_cov: np.ndarray = np.column_stack([np.ones(len(phenotypes)), covariates])
    else:
        X_cov: np.ndarray = np.ones((len(phenotypes), 1))

    # Предварительное вычисление (X_cov^T * X_cov)^-1 для эффективности
    X_cov_T_X_cov_inv: np.ndarray = np.linalg.inv(X_cov.T @ X_cov)


    range_list = list(range(0, n_variants, chunk_size))
    args = [(genotypes, n_variants, n_samples, variant_ids, phenotypes, chunk_size, X_cov, X_cov_T_X_cov_inv, start) for start in range_list]
    with multiprocessing.Pool(processes=num_cpu) as pool:
        gwas_results = pool.starmap(process_chunk, args)
        pool.close()
        pool.join()

        counter.value = 0

    return pd.DataFrame(gwas_results)
