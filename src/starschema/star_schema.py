from __future__ import annotations

from pathlib import Path

import pandas as pd

FLAT_REQUIRED_COLUMNS = [
    "annee_election",
    "tour",
    "code_departement",
    "libelle_departement",
    "code_commune",
    "libelle_commune",
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

PARTICIPATION_COLUMNS = ["inscrits", "abstentions", "votants", "blancs_nuls", "exprimes"]
RESULT_COLUMNS = ["voix"]


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
        "code_nuance",
        "nom",
        "prenom",
        "liste",
    ]:
        stage[col] = _normalize_text(stage[col])

    stage["code_commune"] = _normalize_code_commune(stage["code_commune"])

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

    for col in ["annee_election", "tour", *PARTICIPATION_COLUMNS, *RESULT_COLUMNS]:
        stage[col] = pd.to_numeric(stage[col], errors="coerce").astype("Int64")

    dim_geographie = stage[["code_commune", "code_departement", "libelle_departement", "libelle_commune"]].copy()
    dim_geographie = _assign_surrogate_key(dim_geographie, nk_col="code_commune", sk_col="geo_sk")
    dim_geographie = dim_geographie[["geo_sk", "code_commune", "code_departement", "libelle_departement", "libelle_commune"]]
    if include_unknown_members:
        dim_geographie = _add_unknown_row(dim_geographie, sk_col="geo_sk", nk_col="code_commune", unknown_nk="UNKNOWN")

    dim_election = stage[["election_nk", "annee_election", "tour"]].copy()
    dim_election = _assign_surrogate_key(dim_election, nk_col="election_nk", sk_col="election_sk")
    dim_election = dim_election[["election_sk", "election_nk", "annee_election", "tour"]]
    if include_unknown_members:
        dim_election = _add_unknown_row(dim_election, sk_col="election_sk", nk_col="election_nk", unknown_nk="ELECTION_UNKNOWN")

    dim_candidat_liste = stage[["candidat_nk", "code_nuance", "nom", "prenom", "liste"]].copy()
    dim_candidat_liste = _assign_surrogate_key(dim_candidat_liste, nk_col="candidat_nk", sk_col="candidat_sk")
    dim_candidat_liste = dim_candidat_liste[["candidat_sk", "candidat_nk", "code_nuance", "nom", "prenom", "liste"]]
    if include_unknown_members:
        dim_candidat_liste = _add_unknown_row(dim_candidat_liste, sk_col="candidat_sk", nk_col="candidat_nk", unknown_nk="CANDIDAT_UNKNOWN")

    fact_participation = (
        stage.groupby(["code_commune", "election_nk"], dropna=False)[PARTICIPATION_COLUMNS].max().reset_index()
    )
    fact_participation = fact_participation.merge(
        dim_geographie[["geo_sk", "code_commune"]], on="code_commune", how="left", validate="m:1"
    )
    fact_participation = fact_participation.merge(
        dim_election[["election_sk", "election_nk"]], on="election_nk", how="left", validate="m:1"
    )
    for fk in ["geo_sk", "election_sk"]:
        if include_unknown_members:
            fact_participation[fk] = fact_participation[fk].fillna(0).astype("Int64")
        else:
            fact_participation[fk] = fact_participation[fk].astype("Int64")

    fact_participation = fact_participation[["election_sk", "geo_sk", *PARTICIPATION_COLUMNS]].copy()
    fact_participation.insert(0, "participation_id", pd.RangeIndex(start=1, stop=len(fact_participation) + 1, step=1))

    fact_resultats_liste = (
        stage.groupby(["code_commune", "election_nk", "candidat_nk"], dropna=False)["voix"].sum(min_count=1).reset_index()
    )
    fact_resultats_liste = fact_resultats_liste.merge(
        dim_geographie[["geo_sk", "code_commune"]], on="code_commune", how="left", validate="m:1"
    )
    fact_resultats_liste = fact_resultats_liste.merge(
        dim_election[["election_sk", "election_nk"]], on="election_nk", how="left", validate="m:1"
    )
    fact_resultats_liste = fact_resultats_liste.merge(
        dim_candidat_liste[["candidat_sk", "candidat_nk"]], on="candidat_nk", how="left", validate="m:1"
    )

    for fk in ["geo_sk", "election_sk", "candidat_sk"]:
        if include_unknown_members:
            fact_resultats_liste[fk] = fact_resultats_liste[fk].fillna(0).astype("Int64")
        else:
            fact_resultats_liste[fk] = fact_resultats_liste[fk].astype("Int64")

    fact_resultats_liste = fact_resultats_liste[["election_sk", "geo_sk", "candidat_sk", *RESULT_COLUMNS]].copy()
    fact_resultats_liste.insert(0, "resultat_liste_id", pd.RangeIndex(start=1, stop=len(fact_resultats_liste) + 1, step=1))

    validate_star_schema(
        {
            "dim_geographie": dim_geographie,
            "dim_election": dim_election,
            "dim_candidat_liste": dim_candidat_liste,
            "fact_participation": fact_participation,
            "fact_resultats_liste": fact_resultats_liste,
        }
    )

    return {
        "dim_geographie": dim_geographie,
        "dim_election": dim_election,
        "dim_candidat_liste": dim_candidat_liste,
        "fact_participation": fact_participation,
        "fact_resultats_liste": fact_resultats_liste,
    }



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
    dim_candidat_liste = tables["dim_candidat_liste"]
    fact_participation = tables["fact_participation"]
    fact_resultats_liste = tables["fact_resultats_liste"]

    checks = [
        ("dim_geographie.geo_sk", dim_geographie["geo_sk"].is_unique),
        ("dim_election.election_sk", dim_election["election_sk"].is_unique),
        ("dim_candidat_liste.candidat_sk", dim_candidat_liste["candidat_sk"].is_unique),
    ]

    failed = [name for name, ok in checks if not ok]
    if failed:
        raise ValueError("Cles de dimensions non uniques: " + ", ".join(failed))

    fk_rules = {
        "geo_sk": set(dim_geographie["geo_sk"].dropna().tolist()),
        "election_sk": set(dim_election["election_sk"].dropna().tolist()),
        "candidat_sk": set(dim_candidat_liste["candidat_sk"].dropna().tolist()),
    }

    part_grain_unique = fact_participation[["election_sk", "geo_sk"]].drop_duplicates().shape[0] == len(fact_participation)
    if not part_grain_unique:
        raise ValueError("Grain non unique dans fact_participation (election_sk, geo_sk)")

    for fk_col in ["geo_sk", "election_sk"]:
        bad_fk_count = int((~fact_participation[fk_col].isin(fk_rules[fk_col])).sum())
        if bad_fk_count:
            raise ValueError(f"FK invalide dans fact_participation.{fk_col}: {bad_fk_count} ligne(s)")

    result_grain_unique = (
        fact_resultats_liste[["election_sk", "geo_sk", "candidat_sk"]].drop_duplicates().shape[0]
        == len(fact_resultats_liste)
    )
    if not result_grain_unique:
        raise ValueError("Grain non unique dans fact_resultats_liste (election_sk, geo_sk, candidat_sk)")

    for fk_col in ["geo_sk", "election_sk", "candidat_sk"]:
        bad_fk_count = int((~fact_resultats_liste[fk_col].isin(fk_rules[fk_col])).sum())
        if bad_fk_count:
            raise ValueError(f"FK invalide dans fact_resultats_liste.{fk_col}: {bad_fk_count} ligne(s)")

    if {"inscrits", "votants", "exprimes"}.issubset(fact_participation.columns):
        invalid_logic = (
            (fact_participation["inscrits"] < 0)
            | (fact_participation["abstentions"] < 0)
            | (fact_participation["votants"] < 0)
            | (fact_participation["blancs_nuls"] < 0)
            | (fact_participation["exprimes"] < 0)
            | (fact_participation["votants"] > fact_participation["inscrits"])
            | (fact_participation["exprimes"] > fact_participation["votants"])
            | (fact_participation["blancs_nuls"] > fact_participation["votants"])
        )
        if bool(invalid_logic.fillna(False).any()):
            raise ValueError("Incoherence metrique detectee dans fact_participation")

    if "voix" in fact_resultats_liste.columns:
        invalid_voix = (fact_resultats_liste["voix"] < 0).fillna(False)
        if bool(invalid_voix.any()):
            raise ValueError("Incoherence metrique detectee dans fact_resultats_liste (voix negatives)")


