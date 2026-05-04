from os import path
import pandas as pd
from pandas import DataFrame

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_nuances_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Exports the political nuances data to a new dimension table in Supabase.
    """
    schema_name = 'public'
    table_name = 'dim_nuance_politique'

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    print(f"1. Connecting to Supabase...")
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        
        print(f"2. Exporting data to schema '{schema_name}' as table '{table_name}'...")
        
        loader.export(
            df,
            schema_name,            
            table_name,
            index=False,
            if_exists='replace', 
            drop_table_on_replace=True,  # Keeps us safe from the UndefinedColumn error!
        )
        
    print(f"🎉 Success! The '{table_name}' table is now live in Supabase.")