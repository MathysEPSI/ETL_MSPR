from pathlib import Path
from mage_ai.settings.repo import get_repo_path

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from src.preprocessing.extract_2014 import extract_and_process_2014

DEFAULT_ENCODING = "latin-1"


@data_loader
def load_elections_2014(*args, **kwargs):
    base_path = Path(get_repo_path()) / "raw_data"
    source_path = base_path / kwargs.get("source_file", "MN14_Bvot_T1T2.txt")

    if not source_path.exists():
        raise FileNotFoundError(f"Source introuvable: {source_path}")

    return extract_and_process_2014(
        source_file=str(source_path),
        encoding=kwargs.get("encoding", DEFAULT_ENCODING),
        year=int(kwargs.get("year", 2014)),
    )
