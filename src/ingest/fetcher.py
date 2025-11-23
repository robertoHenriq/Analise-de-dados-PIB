import basedosdados as bd
import pandas as pd
from typing import Optional

def fetch_to_df(query: str, billing_id: str) -> pd.DataFrame:
    if billing_id is None or billing_id == "":
        raise ValueError("billing_id não pode ser None ou vazio para consultar Base dos Dados.")
    df = bd.read_sql(
        query=query,
        billing_project_id=billing_id,
        use_bqstorage_api=True
    )
    return df
