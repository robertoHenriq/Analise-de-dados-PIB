# src/processing/run.py

import os
from pathlib import Path
import pandas as pd
import os, sys
# adiciona a raiz do projeto no path


from src.config import Config
from src.ingest.fetcher import fetch_to_df
from src.processing.processing import (
    etl_extract,
    etl_transform,
    etl_load,
)
from src.processing.clean import clean_dataframe
from src.processing.metrics import export_metrics_json
from src.storage.storage import Storage
from src.queries.queries import PIB_MUNICIPIO_QUERY

from src.processing.logging_utils import setup_logging

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)


# Configura logging
logger = setup_logging()

def transform_fn(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("transform.start", rows_in=len(df))
    df_clean = clean_dataframe(df)
    logger.info("transform.end", rows_out=len(df_clean))
    return df_clean

def save_fn(df: pd.DataFrame) -> str:
    storage = Storage(base_dir=str(Config.DATA_DIR))
    # Salvar com versão automática
    path = storage.save_parquet(df, path="pib_municipio")
    return path

def main():
    logger.info("etl.start")

    billing_id = os.getenv("BILLING_ID") or Config.BILLING_ID
    if not billing_id:
        raise ValueError("BILLING_ID não configurado. Defina via variável de ambiente ou em Config.")

    # Etapa de extração: usa a query da Base dos Dados
    df_raw, metrics_extract = etl_extract(
        fetch_to_df,
        PIB_MUNICIPIO_QUERY,
        billing_id=billing_id
    )

    # Transformação
    df_processed, metrics_transform = etl_transform(df_raw, transform_fn)

    # Load
    saved_path, metrics_load = etl_load(df_processed, save_fn)

    # Exportar métricas
    metrics = {
        "extract": metrics_extract,
        "transform": metrics_transform,
        "load": metrics_load,
    }
    # Salvar métricas em JSON
    storage = Storage(base_dir=str(Config.DATA_DIR))
    metrics_filename = "etl_metrics.json"
    storage.save_json(metrics, metrics_filename)

    logger.info("etl.finished", path=saved_path, metrics=metrics)

if __name__ == "__main__":
    main()
