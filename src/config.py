from pathlib import Path

class Config:
    DATA_DIR = Path("data/")
    DATA_DIR.mkdir(exist_ok=True, parents=True)

    # Se usar Base dos Dados
    BILLING_ID = None  # coloque seu billing_id aqui quando precisar
