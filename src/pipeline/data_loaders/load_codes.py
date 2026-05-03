import pandas as pd
from os import path
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_nuances_csv(*args, **kwargs):
    """
    Reads the nuances_municipales.csv file.
    """
    # Adjust the path if you placed the CSV in a different folder!
    file_path = path.join(get_repo_path(), 'raw_data', 'nuances_municipales.csv')
    
    # Read the CSV. Change sep=";" to sep="," if your CSV uses commas!
    df = pd.read_csv(file_path, sep=",", encoding="utf-8")
    
    # Clean up column names just to be safe (removes accidental spaces)
    df.columns = [col.strip().lower() for col in df.columns]
    
    return df