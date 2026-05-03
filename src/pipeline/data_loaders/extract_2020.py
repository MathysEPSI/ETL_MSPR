from pathlib import Path
from mage_ai.settings.repo import get_repo_path

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from src.preprocessing.extract_2020 import extract_and_process_2020

DEFAULT_ENCODING = "latin-1"

SOURCE_T1 = "2020-05-18-resultats-par-niveau-burvot-t1-france-entiere.txt"
SOURCE_T2 = "resultats-par-niveau-burvot-t2-france-entiere.txt"


@data_loader
def load_elections_2020(*args, **kwargs):
    base_path = Path(get_repo_path()) / "raw_data"
    source_t1_path = base_path / kwargs.get("source_t1", SOURCE_T1)
    source_t2_path = base_path / kwargs.get("source_t2", SOURCE_T2)

    if not source_t1_path.exists() and not source_t2_path.exists():
        raise FileNotFoundError("Les sources sont introuvables.")

    return extract_and_process_2020(
        source_t1=str(source_t1_path),
        source_t2=str(source_t2_path),
        encoding=kwargs.get("encoding", DEFAULT_ENCODING),
        year=int(kwargs.get("year", 2020)),
    )
