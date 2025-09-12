import numpy as np
import pandas as pd
from scipy import stats
import warnings
from tqdm import tqdm


def run_gwas_c(zarr_data, phenotypes, covariates=None, chunk_size=5000):
    """Simplified GWAS using Zarr"""

    genotypes = zarr_data["call_genotype"]
    variant_ids = zarr_data["variant_id"]
    n_variants = genotypes.shape[0]
    n_samples = genotypes.shape[1]

    # Проверка размерностей
    if len(phenotypes) != n_samples:
        raise ValueError(
            f"The dimension of phenotypes ({len(phenotypes)}) does not match the number of samples ({n_samples})"
        )

    if covariates is not None and len(covariates) != n_samples:
        raise ValueError(
            f"The dimension of covariates ({len(covariates)}) does not match the number of samples ({n_samples})"
        )

    gwas_results = []

    # Подготовка ковариат (включая intercept)
    if covariates is not None:
        X_cov = np.column_stack([np.ones(len(phenotypes)), covariates])
    else:
        X_cov = np.ones((len(phenotypes), 1))

    # Предварительное вычисление (X_cov^T * X_cov)^-1 для эффективности
    X_cov_T_X_cov_inv = np.linalg.inv(X_cov.T @ X_cov)

    progress_bar = tqdm(total=n_variants, desc="Processing chunks")
    for start in range(0, n_variants, chunk_size):

        end = min(start + chunk_size, n_variants)
        genotype_chunk = genotypes[start:end]

        for variant_idx in range(genotype_chunk.shape[0]):
            # Получение генотипов для варианта
            gt = genotype_chunk[variant_idx, :, :]

            # Кодирование генотипов (0,1,2) - сумма аллелей
            # Правильное кодирование: 0 = ref/ref, 1 = ref/alt, 2 = alt/alt
            # Учитываем пропущенные значения (-1)
            valid_gt_mask = (gt != -1).all(axis=1)
            dosage = np.zeros(n_samples)

            # Для валидных генотипов считаем дозу альтернативного аллеля
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
                proj_y = X_cov @ (X_cov_T_X_cov_inv @ (X_cov.T @ phenotypes))
                y_resid = phenotypes - proj_y

                # Проекция генотипов на пространство ковариат
                proj_x = X_cov @ (X_cov_T_X_cov_inv @ (X_cov.T @ dosage_normalized))
                x_resid = dosage_normalized - proj_x

                beta = np.sum(x_resid * y_resid) / np.sum(x_resid**2)

                # Остатки и стандартная ошибка
                residuals = y_resid - beta * x_resid
                mse = np.sum(residuals**2) / (len(phenotypes) - X_cov.shape[1] - 1)
                se = np.sqrt(mse / np.sum(x_resid**2))

                # t-статистика и p-value
                t_stat = beta / se
                df = len(phenotypes) - X_cov.shape[1] - 1
                p_value = 2 * (1 - stats.t.cdf(abs(t_stat), df))

                gwas_results.append(
                    {
                        "variant_index": start + variant_idx,
                        "variant_id": variant_ids[start + variant_idx],
                        "beta": beta,
                        "se": se,
                        "p_value": p_value,
                        "t_stat": t_stat,
                        "maf": np.mean(dosage) / 2,  # Minor allele frequency
                    }
                )

            except np.linalg.LinAlgError:
                # Пропускаем варианты
                continue
            except Exception as e:
                warnings.warn(
                    f"Error processing variant {start + variant_idx}: {e}"
                )
                continue
        
        progress_bar.update(end - start)
    progress_bar.close()
    return pd.DataFrame(gwas_results)
