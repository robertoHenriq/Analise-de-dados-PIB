# src/processing/processing.py

from .clean import clean_dataframe
from .metrics import collect_step_metrics, export_metrics_json
from .logging import setup_logging

logger = setup_logging()
    

def etl_extract(fetch_fn, *args, **kwargs):
    df = fetch_fn(*args, **kwargs)
    metrics = collect_step_metrics(step="extract", df_in=df, df_out=df)
    logger.info("etl.step", **metrics)
    return df, metrics


def etl_transform(df, transform_fn):
    df_in = df.copy()
    df_out = transform_fn(df)
    metrics = collect_step_metrics(step="transform", df_in=df_in, df_out=df_out)
    logger.info("etl.step", **metrics)
    return df_out, metrics


def etl_load(df, save_fn):
    df_in = df.copy()
    saved_path = save_fn(df)
    metrics = collect_step_metrics(step="load", df_in=df_in, df_out=df)
    logger.info("etl.step", saved_path=saved_path, **metrics)
    return saved_path, metrics
