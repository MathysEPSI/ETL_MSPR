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
    "code_bureau_vote",
    "inscrits",
    "abstentions",
    "pct_abs_ins",
    "votants",
    "pct_vot_ins",
    "blancs_nuls",
    "pct_blnuls_ins",
    "pct_blnuls_vot",
    "exprimes",
    "pct_exp_ins",
    "pct_exp_vot",
    "code_nuance",
    "sexe",
    "nom",
    "prenom",
    "liste",
    "voix",
    "pct_voix_ins",
    "pct_voix_exp",
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

DECIMAL_COLUMNS = [
    "pct_abs_ins",
    "pct_vot_ins",
    "pct_blnuls_ins",
    "pct_blnuls_vot",
    "pct_exp_ins",
    "pct_exp_vot",
    "pct_voix_ins",
    "pct_voix_exp",
]


def to_int(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.Series(numeric, index=series.index, dtype="Int64")


def to_decimal(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series.astype(str).str.replace(",", ".", regex=False), errors="coerce")
    return pd.Series(numeric, index=series.index, dtype="Float64")


def percent(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    result = (numerator.astype("Float64") * 100.0) / denominator.astype("Float64")
    result = result.where(denominator.notna() & (denominator != 0))
    return result.round(2)


def normalize_code(series: pd.Series, width: int) -> pd.Series:
    values = series.astype("string").str.strip()
    missing_mask = values.isna() | values.eq("") | values.str.upper().isin({"NA", "NAN", "NONE"})
    numeric_mask = values.str.fullmatch(r"\d+").fillna(False)
    normalized = values.where(~numeric_mask, values.str.zfill(width))
    return normalized.mask(missing_mask)


def normalize_code_nuance(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip()
    missing_mask = values.isna() | values.eq("") | values.str.upper().isin({"NA", "NAN", "NONE"})
    return values.mask(missing_mask, "NC")


def normalize_commune_code(code_departement: pd.Series, code_commune: pd.Series) -> pd.Series:
    dept = normalize_code(code_departement, width=2)
    commune = code_commune.astype("string").str.strip()
    missing_mask = commune.isna() | commune.eq("") | commune.str.upper().isin({"NA", "NAN", "NONE"})
    numeric_mask = commune.str.fullmatch(r"\d+").fillna(False)
    short_numeric_mask = numeric_mask & commune.str.len().le(3)

    composite = commune.where(~short_numeric_mask, dept + commune.str.zfill(3))
    return composite.mask(missing_mask)


def finalize_output_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    for column in OUTPUT_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA

    frame["code_departement"] = normalize_code(frame["code_departement"], width=2)
    frame["code_commune"] = normalize_commune_code(frame["code_departement"], frame["code_commune"])
    frame["code_nuance"] = normalize_code_nuance(frame["code_nuance"])

    frame = frame.reindex(columns=OUTPUT_COLUMNS)

    for column in DECIMAL_COLUMNS:
        frame[column] = to_decimal(frame[column])

    for column in INTEGER_COLUMNS:
        frame[column] = to_int(frame[column])

    return frame


def write_output_frame(df: pd.DataFrame, output_path: str | Path, file_format: str, encoding: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if file_format == "csv":
        df.to_csv(path, sep=";", index=False, encoding=encoding, decimal=".")
    else:
        df.to_parquet(path, index=False)

