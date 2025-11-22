import pandas as pd

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Remover duplicados
    before = len(df)
    df.drop_duplicates(inplace=True)
    after = len(df)

    return df
