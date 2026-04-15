from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

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


@dataclass(frozen=True)
class GeoJoinConfig:
    code_commune_col: str | None = None
    code_postal_col: str | None = None
    ville_col: str | None = None


def _normalize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def _normalize_upper(series: pd.Series) -> pd.Series:
    return _normalize_text(series).str.upper()


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value is None:
            continue
        text = value.strip()
        if text:
            return text
    return None


def _build_geo_nk_from_columns(
    df: pd.DataFrame,
    code_commune_col: str | None,
    code_postal_col: str | None,
    ville_col: str | None,
) -> pd.Series:
    code_commune = _normalize_upper(df[code_commune_col]) if code_commune_col and code_commune_col in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")
    code_postal = _normalize_upper(df[code_postal_col]) if code_postal_col and code_postal_col in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")
    ville = _normalize_upper(df[ville_col]) if ville_col and ville_col in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")

    geo_nk = pd.Series("GEO_UNKNOWN", index=df.index, dtype="string")

    commune_mask = code_commune.notna() & code_commune.ne("")
    geo_nk = geo_nk.mask(commune_mask, "GEO_COMMUNE:" + code_commune)

    cp_ville_mask = (~commune_mask) & code_postal.notna() & code_postal.ne("") & ville.notna() & ville.ne("")
    geo_nk = geo_nk.mask(cp_ville_mask, "GEO_CP_CITY:" + code_postal + "|" + ville)

    ville_mask = (~commune_mask) & (~cp_ville_mask) & ville.notna() & ville.ne("")
    geo_nk = geo_nk.mask(ville_mask, "GEO_CITY:" + ville)

    return geo_nk


def _add_unknown_row(df: pd.DataFrame, sk_col: str, nk_col: str, unknown_nk: str = "UNKNOWN") -> pd.DataFrame:
    unknown_row: dict[str, object] = {column: pd.NA for column in df.columns}
    unknown_row[sk_col] = 0
    unknown_row[nk_col] = unknown_nk
    return pd.concat([pd.DataFrame([unknown_row]), df], ignore_index=True)


def _assign_surrogate_key(df: pd.DataFrame, nk_col: str, sk_col: str) -> pd.DataFrame:
    out = df.drop_duplicates(subset=[nk_col]).sort_values(nk_col, na_position="last").reset_index(drop=True)
    out[sk_col] = pd.RangeIndex(start=1, stop=len(out) + 1, step=1)
    return out


def build_star_schema(flat_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
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

    stage["geo_nk"] = _build_geo_nk_from_columns(
        stage,
        code_commune_col="code_commune",
        code_postal_col=None,
        ville_col="libelle_commune",
    )

    stage["bureau_nk"] = stage["geo_nk"].fillna("GEO_UNKNOWN") + "|BV:" + _normalize_upper(stage["code_bureau_vote"]).fillna("UNKNOWN")
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

    dim_geographie = stage[["geo_nk", "code_departement", "libelle_departement", "code_commune", "libelle_commune"]].copy()
    dim_geographie["code_postal"] = pd.NA
    dim_geographie = _assign_surrogate_key(dim_geographie, nk_col="geo_nk", sk_col="geo_sk")
    dim_geographie = dim_geographie[["geo_sk", "geo_nk", "code_postal", "code_departement", "libelle_departement", "code_commune", "libelle_commune"]]
    dim_geographie = _add_unknown_row(dim_geographie, sk_col="geo_sk", nk_col="geo_nk", unknown_nk="GEO_UNKNOWN")

    dim_election = stage[["election_nk", "annee_election", "tour"]].copy()
    dim_election = _assign_surrogate_key(dim_election, nk_col="election_nk", sk_col="election_sk")
    dim_election = dim_election[["election_sk", "election_nk", "annee_election", "tour"]]
    dim_election = _add_unknown_row(dim_election, sk_col="election_sk", nk_col="election_nk", unknown_nk="ELECTION_UNKNOWN")

    dim_bureau_vote = stage[["bureau_nk", "geo_nk", "code_bureau_vote"]].copy()
    dim_bureau_vote = _assign_surrogate_key(dim_bureau_vote, nk_col="bureau_nk", sk_col="bureau_sk")
    dim_bureau_vote = dim_bureau_vote.merge(
        dim_geographie[["geo_sk", "geo_nk"]],
        on="geo_nk",
        how="left",
        validate="m:1",
    )
    dim_bureau_vote["geo_sk"] = dim_bureau_vote["geo_sk"].fillna(0).astype("Int64")
    dim_bureau_vote = dim_bureau_vote[["bureau_sk", "bureau_nk", "geo_sk", "code_bureau_vote"]]
    dim_bureau_vote = _add_unknown_row(dim_bureau_vote, sk_col="bureau_sk", nk_col="bureau_nk", unknown_nk="BUREAU_UNKNOWN")
    dim_bureau_vote.loc[0, "geo_sk"] = 0

    dim_candidat_liste = stage[["candidat_nk", "code_nuance", "nom", "prenom", "liste"]].copy()
    dim_candidat_liste = _assign_surrogate_key(dim_candidat_liste, nk_col="candidat_nk", sk_col="candidat_sk")
    dim_candidat_liste = dim_candidat_liste[["candidat_sk", "candidat_nk", "code_nuance", "nom", "prenom", "liste"]]
    dim_candidat_liste = _add_unknown_row(dim_candidat_liste, sk_col="candidat_sk", nk_col="candidat_nk", unknown_nk="CANDIDAT_UNKNOWN")

    fact = stage[["geo_nk", "bureau_nk", "election_nk", "candidat_nk", *MEASURE_COLUMNS]].copy()

    fact = fact.merge(dim_geographie[["geo_sk", "geo_nk"]], on="geo_nk", how="left", validate="m:1")
    fact = fact.merge(dim_bureau_vote[["bureau_sk", "bureau_nk"]], on="bureau_nk", how="left", validate="m:1")
    fact = fact.merge(dim_election[["election_sk", "election_nk"]], on="election_nk", how="left", validate="m:1")
    fact = fact.merge(dim_candidat_liste[["candidat_sk", "candidat_nk"]], on="candidat_nk", how="left", validate="m:1")

    for fk in ["geo_sk", "bureau_sk", "election_sk", "candidat_sk"]:
        fact[fk] = fact[fk].fillna(0).astype("Int64")

    fact = fact[["election_sk", "geo_sk", "bureau_sk", "candidat_sk", *MEASURE_COLUMNS]].copy()
    fact.insert(0, "fact_id", pd.RangeIndex(start=1, stop=len(fact) + 1, step=1))

    validate_star_schema(
        {
            "dim_geographie": dim_geographie,
            "dim_election": dim_election,
            "dim_bureau_vote": dim_bureau_vote,
            "dim_candidat_liste": dim_candidat_liste,
            "fact_resultats_votes": fact,
        }
    )

    return {
        "dim_geographie": dim_geographie,
        "dim_election": dim_election,
        "dim_bureau_vote": dim_bureau_vote,
        "dim_candidat_liste": dim_candidat_liste,
        "fact_resultats_votes": fact,
    }


def register_geo_metrics_dataset(
    dataset_name: str,
    metrics_df: pd.DataFrame,
    dim_geographie: pd.DataFrame,
    metric_columns: Iterable[str],
    join_config: GeoJoinConfig,
) -> pd.DataFrame:
    out = metrics_df.copy()

    out["geo_nk"] = _build_geo_nk_from_columns(
        out,
        code_commune_col=join_config.code_commune_col,
        code_postal_col=join_config.code_postal_col,
        ville_col=join_config.ville_col,
    )

    metrics_cols = list(metric_columns)
    missing_metrics = [col for col in metrics_cols if col not in out.columns]
    if missing_metrics:
        missing_text = ", ".join(missing_metrics)
        raise ValueError(f"Colonnes metriques manquantes pour {dataset_name}: {missing_text}")

    result = out[["geo_nk", *metrics_cols]].copy()
    result = result.merge(dim_geographie[["geo_sk", "geo_nk"]], on="geo_nk", how="left", validate="m:1")
    result["geo_sk"] = result["geo_sk"].fillna(0).astype("Int64")
    result.insert(0, "dataset_name", dataset_name)
    result.insert(1, "metric_row_id", pd.RangeIndex(start=1, stop=len(result) + 1, step=1))

    return result[["dataset_name", "metric_row_id", "geo_sk", "geo_nk", *metrics_cols]]


def export_tables_csv(
    tables: dict[str, pd.DataFrame],
    output_dir: str | Path,
    sep: str = ";",
    encoding: str = "utf-8",
) -> None:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)

    for table_name, table_df in tables.items():
        out_path = target / f"{table_name}.csv"
        table_df.to_csv(out_path, sep=sep, index=False, encoding=encoding)


def export_tables_dataframes(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {name: df.copy() for name, df in tables.items()}


def validate_star_schema(tables: dict[str, pd.DataFrame]) -> None:
    dim_geographie = tables["dim_geographie"]
    dim_election = tables["dim_election"]
    dim_bureau_vote = tables["dim_bureau_vote"]
    dim_candidat_liste = tables["dim_candidat_liste"]
    fact = tables["fact_resultats_votes"]

    checks = [
        ("dim_geographie.geo_sk", dim_geographie["geo_sk"].is_unique),
        ("dim_election.election_sk", dim_election["election_sk"].is_unique),
        ("dim_bureau_vote.bureau_sk", dim_bureau_vote["bureau_sk"].is_unique),
        ("dim_candidat_liste.candidat_sk", dim_candidat_liste["candidat_sk"].is_unique),
    ]

    failed = [name for name, ok in checks if not ok]
    if failed:
        raise ValueError("Cles de dimensions non uniques: " + ", ".join(failed))

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

