from __future__ import annotations

from pathlib import Path

import pandas as pd

DEFAULT_ENCODING = "latin-1"
DEFAULT_OUTPUT_STEM = "elections_flat"

OUTPUT_COLUMNS = [
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

INTEGER_COLUMNS = [
    "annee_election",
    "tour",
    "inscrits",
    "abstentions",
    "votants",
    "blancs_nuls",
    "exprimes",
    "voix",
]

PARSER_REQUIRED_COLUMNS = [
    "annee_election",
    "tour",
    "code_departement",
    "code_commune",
    "inscrits",
    "votants",
    "exprimes",
    "voix",
]

COMMUNE_KEY_COLUMNS = [
    "annee_election",
    "tour",
    "code_departement",
    "libelle_departement",
    "code_commune",
    "libelle_commune",
]

CANDIDATE_KEY_COLUMNS = [
    "code_nuance",
    "nom",
    "prenom",
    "liste",
]

TOTAL_MEASURE_COLUMNS = ["inscrits", "abstentions", "votants", "blancs_nuls", "exprimes"]


def to_int(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.Series(numeric, index=series.index, dtype="Int64")


def normalize_code(series: pd.Series, width: int) -> pd.Series:
    values = series.astype("string").str.strip().str.upper()
    missing_mask = values.isna() | values.eq("") | values.str.upper().isin({"NA", "NAN", "NONE"})
    numeric_mask = values.str.fullmatch(r"\d+").fillna(False)
    normalized = values.where(~numeric_mask, values.str.zfill(width))
    return normalized.mask(missing_mask)


def normalize_code_nuance(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip().str.upper()
    missing_mask = values.isna() | values.eq("") | values.str.upper().isin({"NA", "NAN", "NONE"})
    return values.mask(missing_mask, "NC")


def normalize_commune_code(code_departement: pd.Series, code_commune: pd.Series) -> pd.Series:
    dept = normalize_code(code_departement, width=2)
    commune = code_commune.astype("string").str.strip().str.upper()
    missing_mask = (
        commune.isna()
        | commune.eq("")
        | commune.str.upper().isin({"NA", "NAN", "NONE"})
        | dept.isna()
    )
    numeric_mask = commune.str.fullmatch(r"\d+").fillna(False)
    short_numeric_mask = numeric_mask & commune.str.len().le(3)

    composite = commune.where(~short_numeric_mask, dept + commune.str.zfill(3))
    return composite.mask(missing_mask)


def is_overseas_department(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip().str.upper()
    return values.str.fullmatch(r"Z.").fillna(False) | values.str.fullmatch(r"9\d\d").fillna(False)


def filter_mainland_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "code_departement" not in df.columns:
        return df
    return df.loc[~is_overseas_department(df["code_departement"])].copy()


def filter_common_communes(df: pd.DataFrame, years: list[int] | None = None) -> pd.DataFrame:
    if df.empty or "code_commune" not in df.columns or "annee_election" not in df.columns:
        return df

    scope_years = years if years is not None else sorted(df["annee_election"].dropna().unique().tolist())
    if not scope_years:
        return df

    scoped = df.loc[df["annee_election"].isin(scope_years)].copy()
    if scoped.empty:
        return scoped

    commune_years = (
        scoped.dropna(subset=["code_commune", "annee_election"])
        .groupby("code_commune")["annee_election"]
        .nunique()
    )
    valid_communes = commune_years[commune_years == len(scope_years)].index
    return scoped.loc[scoped["code_commune"].isin(valid_communes)].copy()


def filter_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    required_present = [col for col in PARSER_REQUIRED_COLUMNS if col in df.columns]
    if not required_present:
        return df

    # Drop rows that cannot be trusted after parsing (missing structural keys).
    invalid_mask = df[required_present].isna().any(axis=1)

    if "code_departement" in df.columns:
        invalid_mask = invalid_mask | ~df["code_departement"].astype("string").str.fullmatch(r"[0-9A-Z]{2,3}").fillna(False)
    if "code_commune" in df.columns:
        invalid_mask = invalid_mask | ~df["code_commune"].astype("string").str.fullmatch(r"[0-9A-Z]{5}").fillna(False)

    if {"inscrits", "votants", "exprimes", "voix"}.issubset(df.columns):
        invalid_mask = invalid_mask | (df["inscrits"] < 0) | (df["votants"] < 0) | (df["exprimes"] < 0) | (df["voix"] < 0)
        invalid_mask = invalid_mask | (df["votants"] > df["inscrits"]) | (df["exprimes"] > df["votants"]) | (df["voix"] > df["exprimes"])

    # Requested business rule: remove rows with no turnout.
    if "votants" in df.columns:
        invalid_mask = invalid_mask | (df["votants"] == 0)

    return df.loc[~invalid_mask].copy()


def aggregate_to_commune(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    frame = df.copy()
    for col in [*COMMUNE_KEY_COLUMNS, *CANDIDATE_KEY_COLUMNS, *TOTAL_MEASURE_COLUMNS, "voix"]:
        if col not in frame.columns:
            frame[col] = pd.NA

    for col in [*TOTAL_MEASURE_COLUMNS, "voix"]:
        frame[col] = to_int(frame[col])

    if "code_bureau_vote" in frame.columns:
        bureau_rollup = (
            frame.groupby([*COMMUNE_KEY_COLUMNS, "code_bureau_vote"], dropna=False)[TOTAL_MEASURE_COLUMNS]
            .max()
            .reset_index()
        )
        commune_totals = bureau_rollup.groupby(COMMUNE_KEY_COLUMNS, dropna=False)[TOTAL_MEASURE_COLUMNS].sum().reset_index()
    else:
        commune_totals = frame.groupby(COMMUNE_KEY_COLUMNS, dropna=False)[TOTAL_MEASURE_COLUMNS].max().reset_index()

    candidate_scores = (
        frame.groupby([*COMMUNE_KEY_COLUMNS, *CANDIDATE_KEY_COLUMNS], dropna=False)["voix"].sum(min_count=1).reset_index()
    )

    return candidate_scores.merge(commune_totals, on=COMMUNE_KEY_COLUMNS, how="left", validate="m:1")


def finalize_output_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = filter_mainland_rows(df.copy())

    for column in ["code_departement", "code_commune", "code_nuance"]:
        if column not in frame.columns:
            frame[column] = pd.NA

    frame["code_departement"] = normalize_code(frame["code_departement"], width=2)
    frame["code_commune"] = normalize_commune_code(frame["code_departement"], frame["code_commune"])
    if "code_bureau_vote" in frame.columns:
        frame["code_bureau_vote"] = normalize_code(frame["code_bureau_vote"], width=4)
    frame["code_nuance"] = normalize_code_nuance(frame["code_nuance"])

    frame = aggregate_to_commune(frame)

    for column in OUTPUT_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA

    frame = frame.reindex(columns=OUTPUT_COLUMNS)


    for column in INTEGER_COLUMNS:
        frame[column] = to_int(frame[column])

    return filter_invalid_rows(frame)


def write_output_frame(df: pd.DataFrame, output_path: str | Path, file_format: str, encoding: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if file_format == "csv":
        df.to_csv(path, sep=";", index=False, encoding=encoding, decimal=".", errors="replace")
    else:
        df.to_parquet(path, index=False)

