from __future__ import annotations

import sys
from os import path
from pandas import DataFrame

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres

sys.path.insert(0, get_repo_path())

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from src.starschema.star_schema import build_star_schema

SCHEMA_NAME = 'public'

# Dimensions must be exported before facts (FK dependency order)
TABLE_ORDER = [
    'dim_geographie',
    'dim_election',
    'dim_candidat_liste',
    'fact_participation',
    'fact_resultats_liste',
]


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Builds a commune-level star schema using src.starschema.star_schema
    and exports all 5 tables to Supabase via Postgres.

    Tables produced:
      - dim_geographie
      - dim_election
      - dim_candidat_liste
      - fact_participation    (1 row = 1 commune × 1 tour × 1 annee)
      - fact_resultats_liste  (1 row = 1 candidat/liste × 1 commune × 1 tour × 1 annee)
    """
    print("1. Building commune-level star schema...")
    tables = build_star_schema(df, include_unknown_members=False)

    for name, table in tables.items():
        print(f"   {name}: {len(table)} rows")

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    print("2. Connecting to Supabase...")
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for table_name in TABLE_ORDER:
            print(f"   Exporting {table_name} → schema '{SCHEMA_NAME}'...")
            loader.export(
                tables[table_name],
                SCHEMA_NAME,
                table_name,
                index=False,
                if_exists='replace',
                drop_table_on_replace=True,
            )

    print("🎉 Pipeline Complete! Star Schema successfully exported to Supabase.")