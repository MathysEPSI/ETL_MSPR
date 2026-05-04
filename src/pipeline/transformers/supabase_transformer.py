import sys
import pandas as pd
from mage_ai.settings.repo import get_repo_path

sys.path.insert(0, get_repo_path())

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer

from src.preprocessing.common import filter_common_communes


@transformer
def merge_dataframes(*args, **kwargs):
    """
    Takes all connected upstream dataframes and stacks them vertically.
    Applies the common-communes filtering logic from src.preprocessing.common
    to keep only communes present across all processed election years.
    """
    if not args:
        return pd.DataFrame()

    combined_df = pd.concat(args, ignore_index=True)

    processed_years = sorted(combined_df["annee_election"].dropna().unique().tolist())

    if processed_years:
        combined_df = filter_common_communes(combined_df, years=processed_years)

    return combined_df
