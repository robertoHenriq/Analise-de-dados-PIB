from pathlib import Path
import pandas as pd
from typing import Optional

from src.processing.logging import setup_logging

logger = setup_logging("INFO")


class Storage:
    """
    Classe responsável por salvar e carregar datasets em formatos variados.
    Suporta versionamento automático para Parquet.
    """

    def __init__(self, base_dir: str = "data/"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True, parents=True)

        logger.info("storage.init", base_dir=str(self.base_dir))

    # ----------------------------------------------------------------------
    # Helpers internos
    # ----------------------------------------------------------------------
    def _ensure_dir(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)

    def _increment_version(self, base_path: Path) -> Path:
        """
        Gera automaticamente o próximo arquivo versionado.
        Exemplo:
            input: data/output/file.parquet
            output: data/output/file.v3.parquet
        """
        existing = sorted(base_path.parent.glob(base_path.stem + ".v*.parquet"))

        if not existing:
            version = 1
        else:
            last = existing[-1].stem
            version = int(last.split(".v")[-1]) + 1

        new_path = base_path.with_suffix(f".v{version}.parquet")
        return new_path

    # ----------------------------------------------------------------------
    # Métodos de salvamento
    # ----------------------------------------------------------------------
    def save_parquet(
        self,
        df: pd.DataFrame,
        path: str,
        version: Optional[int] = None,
        auto_version: bool = True,
    ) -> str:
        """
        Salva DataFrame em parquet com versionamento automático.
        """
        base = self.base_dir / path
        self._ensure_dir(base)

        if version is not None:
            filepath = base.with_suffix(f".v{version}.parquet")
        elif auto_version:
            filepath = self._increment_version(base)
        else:
            filepath = base.with_suffix(".parquet")

        df.to_parquet(filepath, index=False)

        logger.info(
            "storage.save_parquet",
            path=str(filepath),
            rows=len(df),
            auto_version=auto_version,
        )

        return str(filepath)

    def save_csv(self, df: pd.DataFrame, path: str) -> str:
        """
        Salva DataFrame como CSV simples.
        """
        filepath = self.base_dir / path
        self._ensure_dir(filepath)

        df.to_csv(filepath, index=False)

        logger.info("storage.save_csv", path=str(filepath), rows=len(df))
        return str(filepath)

    def save_json(self, obj: dict, path: str) -> str:
        """
        Salva JSON (ex.: exportação de métricas)
        """
        filepath = self.base_dir / path
        self._ensure_dir(filepath)

        import json
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=4)

        logger.info("storage.save_json", path=str(filepath))
        return str(filepath)

    # ----------------------------------------------------------------------
    # Métodos de
