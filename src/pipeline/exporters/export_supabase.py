from __future__ import annotations

from os import path
import pandas as pd
from pandas import DataFrame

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


FLAT_REQUIRED_COLUMNS = [
    "annee_election",
    "tour",
    "code_departement",
    "libelle_departement",
    "code_commune",
    "libelle_commune",
    "code_bureau_vote",
    "inscrits",
    "abstentions",
    "votants",
    "blancs_nuls",
    "exprimes",
    "code_nuance",
    "nom",
    "prenom",
    "liste",
    "voix",
]

MEASURE_COLUMNS = ["inscrits", "abstentions", "votants", "blancs_nuls", "exprimes", "voix"]


def _normalize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def _normalize_upper(series: pd.Series) -> pd.Series:
    return _normalize_text(series).str.upper()


def _normalize_code_commune(series: pd.Series) -> pd.Series:
    out = _normalize_text(series)
    out = out.mask(out.isna() | out.eq(""), pd.NA)
    return out.str.zfill(5)


def _add_unknown_row(df: pd.DataFrame, sk_col: str, nk_col: str, unknown_nk: str = "UNKNOWN") -> pd.DataFrame:
    unknown_row: dict[str, object] = {column: pd.NA for column in df.columns}
    unknown_row[sk_col] = 0
    unknown_row[nk_col] = unknown_nk
    return pd.concat([pd.DataFrame([unknown_row]), df], ignore_index=True)


def _assign_surrogate_key(df: pd.DataFrame, nk_col: str, sk_col: str) -> pd.DataFrame:
    out = df.drop_duplicates(subset=[nk_col]).sort_values(nk_col, na_position="last").reset_index(drop=True)
    out[sk_col] = pd.RangeIndex(start=1, stop=len(out) + 1, step=1)
    return out


def validate_star_schema(tables: dict[str, pd.DataFrame]) -> None:
    dim_geographie = tables["dim_geographie"]
    dim_election = tables["dim_election"]
    dim_bureau_vote = tables["dim_bureau_vote"]
    dim_candidat_liste = tables["dim_candidat_liste"]
    fact = tables["fact_resultats_votes"]

    # 1. Uniqueness Checks
    checks = [
        ("dim_geographie.geo_sk", dim_geographie["geo_sk"].is_unique),
        ("dim_election.election_sk", dim_election["election_sk"].is_unique),
        ("dim_bureau_vote.bureau_sk", dim_bureau_vote["bureau_sk"].is_unique),
        ("dim_candidat_liste.candidat_sk", dim_candidat_liste["candidat_sk"].is_unique),
    ]

    failed = [name for name, ok in checks if not ok]
    if failed:
        raise ValueError("Cles de dimensions non uniques: " + ", ".join(failed))

    # 2. Referential Integrity Checks
    fk_rules = {
        "geo_sk": set(dim_geographie["geo_sk"].dropna().tolist()),
        "election_sk": set(dim_election["election_sk"].dropna().tolist()),
        "bureau_sk": set(dim_bureau_vote["bureau_sk"].dropna().tolist()),
        "candidat_sk": set(dim_candidat_liste["candidat_sk"].dropna().tolist()),
    }

    for fk_col, valid_values in fk_rules.items():
        bad_fk_count = int((~fact[fk_col].isin(valid_values)).sum())
        if bad_fk_count:
            raise ValueError(f"FK invalide dans fact_resultats_votes.{fk_col}: {bad_fk_count} ligne(s)")

    # 3. Metric Coherence Checks
    if {"inscrits", "votants", "exprimes", "voix"}.issubset(fact.columns):
        invalid_logic = (
            (fact["inscrits"] < 0)
            | (fact["votants"] < 0)
            | (fact["exprimes"] < 0)
            | (fact["voix"] < 0)
            | (fact["votants"] > fact["inscrits"])
            | (fact["exprimes"] > fact["votants"])
            | (fact["voix"] > fact["exprimes"])
        )
        if bool(invalid_logic.fillna(False).any()):
            raise ValueError("Incoherence metrique detectee dans fact_resultats_votes")


def build_star_schema(flat_df: pd.DataFrame, include_unknown_members: bool = False) -> dict[str, pd.DataFrame]:
    missing = [col for col in FLAT_REQUIRED_COLUMNS if col not in flat_df.columns]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"Colonnes manquantes dans elections_flat: {missing_text}")

    stage = flat_df.copy()

    for col in [
        "code_departement",
        "libelle_departement",
        "code_commune",
        "libelle_commune",
        "code_bureau_vote",
        "code_nuance",
        "nom",
        "prenom",
        "liste",
    ]:
        stage[col] = _normalize_text(stage[col])

    stage["code_commune"] = _normalize_code_commune(stage["code_commune"])

    stage["bureau_nk"] = stage["code_commune"].fillna("UNKNOWN") + "|BV:" + _normalize_upper(stage["code_bureau_vote"]).fillna("UNKNOWN")
    stage["election_nk"] = _normalize_text(stage["annee_election"]).fillna("UNKNOWN") + "|T" + _normalize_text(stage["tour"]).fillna("UNKNOWN")
    stage["candidat_nk"] = (
        _normalize_upper(stage["code_nuance"]).fillna("NC")
        + "|"
        + _normalize_upper(stage["nom"]).fillna("UNKNOWN")
        + "|"
        + _normalize_upper(stage["prenom"]).fillna("UNKNOWN")
        + "|"
        + _normalize_upper(stage["liste"]).fillna("UNKNOWN")
    )

    for col in ["annee_election", "tour", *MEASURE_COLUMNS]:
        stage[col] = pd.to_numeric(stage[col], errors="coerce").astype("Int64")

    # Dim Geographie
    dim_geographie = stage[["code_commune", "code_departement", "libelle_departement", "libelle_commune"]].copy()
    dim_geographie = _assign_surrogate_key(dim_geographie, nk_col="code_commune", sk_col="geo_sk")
    dim_geographie = dim_geographie[["geo_sk", "code_commune", "code_departement", "libelle_departement", "libelle_commune"]]
    if include_unknown_members:
        dim_geographie = _add_unknown_row(dim_geographie, sk_col="geo_sk", nk_col="code_commune", unknown_nk="UNKNOWN")

    # Dim Election
    dim_election = stage[["election_nk", "annee_election", "tour"]].copy()
    dim_election = _assign_surrogate_key(dim_election, nk_col="election_nk", sk_col="election_sk")
    dim_election = dim_election[["election_sk", "election_nk", "annee_election", "tour"]]
    if include_unknown_members:
        dim_election = _add_unknown_row(dim_election, sk_col="election_sk", nk_col="election_nk", unknown_nk="ELECTION_UNKNOWN")

    # Dim Bureau Vote
    dim_bureau_vote = stage[["bureau_nk", "code_commune", "code_bureau_vote"]].copy()
    dim_bureau_vote = _assign_surrogate_key(dim_bureau_vote, nk_col="bureau_nk", sk_col="bureau_sk")
    dim_bureau_vote = dim_bureau_vote.merge(
        dim_geographie[["geo_sk", "code_commune"]],
        on="code_commune",
        how="left",
        validate="m:1",
    )
    if include_unknown_members:
        dim_bureau_vote["geo_sk"] = dim_bureau_vote["geo_sk"].fillna(0).astype("Int64")
    else:
        dim_bureau_vote["geo_sk"] = dim_bureau_vote["geo_sk"].astype("Int64")
    dim_bureau_vote = dim_bureau_vote[["bureau_sk", "bureau_nk", "geo_sk", "code_commune", "code_bureau_vote"]]
    if include_unknown_members:
        dim_bureau_vote = _add_unknown_row(dim_bureau_vote, sk_col="bureau_sk", nk_col="bureau_nk", unknown_nk="BUREAU_UNKNOWN")
        dim_bureau_vote.loc[0, "geo_sk"] = 0

    # Dim Candidat Liste
    dim_candidat_liste = stage[["candidat_nk", "code_nuance", "nom", "prenom", "liste"]].copy()
    dim_candidat_liste = _assign_surrogate_key(dim_candidat_liste, nk_col="candidat_nk", sk_col="candidat_sk")
    dim_candidat_liste = dim_candidat_liste[["candidat_sk", "candidat_nk", "code_nuance", "nom", "prenom", "liste"]]
    if include_unknown_members:
        dim_candidat_liste = _add_unknown_row(dim_candidat_liste, sk_col="candidat_sk", nk_col="candidat_nk", unknown_nk="CANDIDAT_UNKNOWN")

    # Fact Table
    fact = stage[["code_commune", "bureau_nk", "election_nk", "candidat_nk", *MEASURE_COLUMNS]].copy()

    fact = fact.merge(dim_geographie[["geo_sk", "code_commune"]], on="code_commune", how="left", validate="m:1")
    fact = fact.merge(dim_bureau_vote[["bureau_sk", "bureau_nk"]], on="bureau_nk", how="left", validate="m:1")
    fact = fact.merge(dim_election[["election_sk", "election_nk"]], on="election_nk", how="left", validate="m:1")
    fact = fact.merge(dim_candidat_liste[["candidat_sk", "candidat_nk"]], on="candidat_nk", how="left", validate="m:1")

    for fk in ["geo_sk", "bureau_sk", "election_sk", "candidat_sk"]:
        if include_unknown_members:
            fact[fk] = fact[fk].fillna(0).astype("Int64")
        else:
            fact[fk] = fact[fk].astype("Int64")

    fact = fact[["election_sk", "geo_sk", "bureau_sk", "candidat_sk", *MEASURE_COLUMNS]].copy()
    fact.insert(0, "fact_id", pd.RangeIndex(start=1, stop=len(fact) + 1, step=1))

    # Validate before returning
    tables = {
        "dim_geographie": dim_geographie,
        "dim_election": dim_election,
        "dim_bureau_vote": dim_bureau_vote,
        "dim_candidat_liste": dim_candidat_liste,
        "fact_resultats_votes": fact,
    }
    
    print("Running Data Quality Validation Checks...")
    validate_star_schema(tables)
    print("Validation Passed!")

    return tables


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Exports the generated Star Schema to Supabase using Mage's Postgres Connector.
    """
    schema_name = 'public' 
    
    print("1. Generating Star Schema Tables using provided logic...")
    tables = build_star_schema(df, include_unknown_members=False)

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Order of tables to export (Dimensions first, Fact table last)
    table_order = [
        'dim_geographie',
        'dim_election',
        'dim_bureau_vote',
        'dim_candidat_liste',
        'fact_resultats_votes'
    ]

    print("2. Connecting to Supabase...")
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        
        for table_name in table_order:
            print(f"Exporting {table_name} to schema '{schema_name}'...")
            loader.export(
                tables[table_name],
                schema_name,            
                table_name,
                index=False,
                if_exists='replace', 
                drop_table_on_replace=True, 
            )
            
    print("🎉 Pipeline Complete! Star Schema successfully exported to Supabase.")