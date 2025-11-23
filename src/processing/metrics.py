# src/processing/metrics.py

import json
import pandas as pd
from typing import Dict, Any


def compute_metrics(before_df: pd.DataFrame, after_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula métricas de transformação entre dois DataFrames:
    - número de linhas antes e depois
    - número de duplicados removidos
    - porcentagem de valores ausentes (missing) antes e depois (média e por coluna)
    - estatísticas de mudança de missing por coluna
    """
    rows_in = len(before_df)
    rows_out = len(after_df)
    duplicated_removed = rows_in - len(before_df.drop_duplicates())

    # Missing global
    missing_before_pct = float(before_df.isna().mean().mean()) * 100
    missing_after_pct = float(after_df.isna().mean().mean()) * 100

    # Missing por coluna (antes e depois)
    missing_by_column = {}
    for col in after_df.columns:
        before_col_pct = float(before_df[col].isna().mean()) * 100 if col in before_df else None
        after_col_pct = float(after_df[col].isna().mean()) * 100
        missing_by_column[col] = {
            "before_pct": before_col_pct,
            "after_pct": after_col_pct
        }

    metrics = {
        "rows_in": rows_in,
        "rows_out": rows_out,
        "duplicated_removed": duplicated_removed,
        "missing_before_pct": round(missing_before_pct, 2),
        "missing_after_pct": round(missing_after_pct, 2),
        "missing_by_column": missing_by_column,
    }

    return metrics


def export_metrics_json(metrics: Dict[str, Any], output_path: str = "metrics/metrics.json") -> str:
    """
    Exporta as métricas para um arquivo JSON, criando diretórios conforme necessário.
    Retorna o caminho do arquivo salvo.
    """
    from pathlib import Path
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    with open(p, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4, ensure_ascii=False)

    return str(p)


    
def collect_step_metrics(step: str, df_in: pd.DataFrame, df_out: pd.DataFrame) -> Dict[str, Any]:
    """
    Coleta métricas para uma etapa de ETL (extract, transform, load).
    Retorna um dicionário com:
      - nome da etapa (step)
      - número de linhas antes (rows_in) e depois (rows_out)
      - quantos duplicados foram removidos
      - porcentagem de valores ausentes (missing) antes e depois
    """
    rows_in = len(df_in)
    rows_out = len(df_out)

    # Contar duplicados: número de linhas duplicadas no df_in
    # A ideia é comparar len(df_in) com len(df_in.drop_duplicates())
    duplicates_removed = rows_in - len(df_in.drop_duplicates())

    # Porcentagem de missing (média sobre todas as colunas)
    missing_before_pct = float(df_in.isna().mean().mean()) * 100
    missing_after_pct = float(df_out.isna().mean().mean()) * 100

    metrics = {
        "step": step,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "duplicates_removed": duplicates_removed,
        "missing_before_pct": round(missing_before_pct, 2),
        "missing_after_pct": round(missing_after_pct, 2),
    }

    return metrics


def export_metrics_json(metrics: Dict[str, Any], output_path: str = "metrics/metrics.json") -> str:
    """
    Exporta um dicionário de métricas para um arquivo JSON.
    Retorna o caminho onde salvou.
    """
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    with open(p, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4, ensure_ascii=False)

    return str(p)