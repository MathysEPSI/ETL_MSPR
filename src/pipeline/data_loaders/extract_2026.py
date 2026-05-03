from pathlib import Path
from mage_ai.settings.repo import get_repo_path

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from src.preprocessing.extract_2026 import extract_and_process_2026

SOURCE_T1 = "municipales-2026-resultats-bv-par-communes-2026-03-20.csv"
SOURCE_T2 = "municipales-2026-resultats-bureau-de-vote-2026-03-23-16h15.csv"


@data_loader
def load_elections_2026(*args, **kwargs):
    base_path = Path(get_repo_path()) / "raw_data"
    source_t1_path = base_path / kwargs.get("source_t1", SOURCE_T1)
    source_t2_path = base_path / kwargs.get("source_t2", SOURCE_T2)

    if not source_t1_path.exists() and not source_t2_path.exists():
        raise FileNotFoundError("Les sources sont introuvables.")

    # 2026 files are UTF-8; encoding kwarg can still override if needed.
    return extract_and_process_2026(
        source_t1=str(source_t1_path),
        source_t2=str(source_t2_path),
        encoding=kwargs.get("encoding", "utf-8"),
        year=int(kwargs.get("year", 2026)),
    )
