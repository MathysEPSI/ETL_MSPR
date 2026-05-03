from pathlib import Path
from mage_ai.settings.repo import get_repo_path

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from src.preprocessing.extract_2008 import extract_and_process_2008

DEFAULT_ENCODING = "latin-1"


@data_loader
def load_elections_2008(*args, **kwargs):
    base_path = Path(get_repo_path()) / "raw_data"
    source_path = base_path / kwargs.get("source_file", "MN08_BVot_T1T2.txt")

    if not source_path.exists():
        raise FileNotFoundError(f"Source introuvable: {source_path}")

    return extract_and_process_2008(
        source_file=str(source_path),
        encoding=kwargs.get("encoding", DEFAULT_ENCODING),
        year=int(kwargs.get("year", 2008)),
    )
