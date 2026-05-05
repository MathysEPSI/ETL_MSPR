from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd

REQUIRED_MAPPING_COLUMNS = {
    "source_code",
    "target_table",
    "canonical_metric",
    "data_type",
    "available_years",
}


def _read_mapping(mapping_path: Path, sep: str, encoding: str) -> pd.DataFrame:
    mapping_df = pd.read_csv(mapping_path, sep=sep, encoding=encoding, dtype=str)
    missing_cols = REQUIRED_MAPPING_COLUMNS - set(mapping_df.columns)
    if missing_cols:
        raise ValueError(f"Mapping missing columns: {sorted(missing_cols)}")

    mapping_df = mapping_df[list(REQUIRED_MAPPING_COLUMNS)].copy()
    mapping_df = mapping_df.dropna(subset=["source_code", "target_table", "canonical_metric", "available_years"])

    mapping_df["source_code"] = mapping_df["source_code"].astype(str).str.strip()
    mapping_df["target_table"] = mapping_df["target_table"].astype(str).str.strip()
    mapping_df["canonical_metric"] = mapping_df["canonical_metric"].astype(str).str.strip()
    mapping_df["data_type"] = mapping_df["data_type"].astype(str).str.strip().str.upper()
    mapping_df["available_years"] = mapping_df["available_years"].astype(str).str.strip()

    mapping_df["year"] = pd.to_numeric(mapping_df["available_years"], errors="coerce")
    missing_years = mapping_df["year"].isna().sum()
    if missing_years:
        print(f"Warning: {missing_years} mapping rows have invalid year and will be ignored.")
    mapping_df = mapping_df.dropna(subset=["year"]).copy()
    mapping_df["year"] = mapping_df["year"].astype(int)

    return mapping_df


def _read_meta(meta_path: Path, sep: str, encoding: str) -> pd.DataFrame:
    return pd.read_csv(meta_path, sep=sep, encoding=encoding, dtype=str)


def _read_raw_header(raw_path: Path, sep: str, encoding: str) -> List[str]:
    header_df = pd.read_csv(raw_path, sep=sep, encoding=encoding, nrows=0)
    return list(header_df.columns)


def _filter_mapping_to_raw(mapping_df: pd.DataFrame, raw_columns: Set[str]) -> Tuple[pd.DataFrame, Set[str]]:
    missing_in_raw = set(mapping_df["source_code"]) - raw_columns
    if missing_in_raw:
        print(f"Warning: {len(missing_in_raw)} source_code values not found in raw data.")
        print(missing_in_raw)
    filtered_mapping = mapping_df[mapping_df["source_code"].isin(raw_columns)].copy()
    return filtered_mapping, missing_in_raw


def _warn_missing_in_meta(mapping_df: pd.DataFrame, meta_df: pd.DataFrame) -> None:
    if "COD_VAR" not in meta_df.columns:
        print("Warning: meta file missing COD_VAR column; skipping meta validation.")
        return
    missing_in_meta = set(mapping_df["source_code"]) - set(meta_df["COD_VAR"].astype(str))
    if missing_in_meta:
        print(f"Warning: {len(missing_in_meta)} source_code values not found in meta file.")
        print(missing_in_meta)


def _read_raw_data(
    raw_path: Path,
    usecols: Iterable[str],
    sep: str,
    encoding: str,
    limit_rows: Optional[int],
) -> pd.DataFrame:
    return pd.read_csv(
        raw_path,
        sep=sep,
        encoding=encoding,
        dtype=str,
        usecols=list(usecols),
        nrows=limit_rows,
        low_memory=False,
    )


def _read_dim_geographie_codes(dim_geo_path: Path, sep: str, encoding: str) -> Set[str]:
    dim_df = pd.read_csv(dim_geo_path, sep=sep, encoding=encoding, usecols=["code_commune"], dtype=str)
    if "code_commune" not in dim_df.columns:
        raise ValueError("dim_geographie.csv must include code_commune column.")
    codes = dim_df["code_commune"].dropna().astype(str).str.strip()
    return set(codes.tolist())


def _to_numeric(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")


def _coerce_types(df: pd.DataFrame, data_type_by_metric: Dict[str, str]) -> pd.DataFrame:
    for metric, data_type in data_type_by_metric.items():
        if metric not in df.columns:
            continue
        if data_type == "NUM":
            df[metric] = _to_numeric(df[metric])
    return df


def _sanitize_table_name(table_name: str) -> str:
    return table_name.replace("/", "_").replace("\\", "_")


def flatten_dossier_complet_tables(
    raw_path: Path,
    mapping_path: Path,
    meta_path: Optional[Path] = None,
    dim_geo_path: Optional[Path] = None,
    sep: str = ";",
    encoding: str = "utf-8",
    limit_rows: Optional[int] = None,
    sanitize_table_names: bool = True,
) -> Dict[str, pd.DataFrame]:
    mapping_df = _read_mapping(mapping_path, sep=sep, encoding=encoding)
    raw_columns = set(_read_raw_header(raw_path, sep=sep, encoding=encoding))

    if "CODGEO" not in raw_columns:
        raise ValueError("Raw data must include CODGEO column.")

    mapping_df, _missing = _filter_mapping_to_raw(mapping_df, raw_columns)
    if mapping_df.empty:
        raise ValueError("No mapping rows matched raw data columns.")

    if meta_path is not None:
        meta_df = _read_meta(meta_path, sep=sep, encoding=encoding)
        _warn_missing_in_meta(mapping_df, meta_df)

    usecols = {"CODGEO"} | set(mapping_df["source_code"])
    raw_df = _read_raw_data(raw_path, usecols=usecols, sep=sep, encoding=encoding, limit_rows=limit_rows)

    if dim_geo_path is not None:
        allowed_codes = _read_dim_geographie_codes(dim_geo_path, sep=sep, encoding=encoding)
        before_count = len(raw_df)
        raw_df = raw_df[raw_df["CODGEO"].isin(allowed_codes)].copy()
        removed_count = before_count - len(raw_df)
        if removed_count:
            print(f"Filtered {removed_count} rows with CODGEO not in dim_geographie.")

    tables: Dict[str, pd.DataFrame] = {}
    for table_name, table_mapping in mapping_df.groupby("target_table"):
        canonical_order = list(dict.fromkeys(table_mapping["canonical_metric"].tolist()))
        table_frames: List[pd.DataFrame] = []

        for year, year_mapping in table_mapping.groupby("year"):
            source_cols = year_mapping["source_code"].tolist()
            rename_map = dict(zip(source_cols, year_mapping["canonical_metric"]))

            df_out = raw_df[["CODGEO", *source_cols]].copy()
            df_out = df_out.rename(columns=rename_map)
            df_out.insert(1, "annee", int(year))

            data_type_by_metric = dict(
                zip(year_mapping["canonical_metric"], year_mapping["data_type"])
            )
            df_out = _coerce_types(df_out, data_type_by_metric)

            table_frames.append(df_out)

        table_df = pd.concat(table_frames, ignore_index=True)
        table_df = table_df.reindex(columns=["CODGEO", "annee", *canonical_order])
        table_df = table_df.sort_values(by=["CODGEO", "annee"], kind="mergesort")

        safe_table_name = _sanitize_table_name(table_name) if sanitize_table_names else table_name
        tables[safe_table_name] = table_df

    return tables


def flatten_dossier_complet(
    raw_path: Path,
    mapping_path: Path,
    output_dir: Path,
    meta_path: Optional[Path] = None,
    dim_geo_path: Optional[Path] = None,
    sep: str = ";",
    encoding: str = "utf-8",
    limit_rows: Optional[int] = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = flatten_dossier_complet_tables(
        raw_path=raw_path,
        mapping_path=mapping_path,
        meta_path=meta_path,
        dim_geo_path=dim_geo_path,
        sep=sep,
        encoding=encoding,
        limit_rows=limit_rows,
        sanitize_table_names=True,
    )

    for table_name, table_df in tables.items():
        output_path = output_dir / f"{table_name}.csv"
        table_df.to_csv(output_path, sep=sep, encoding=encoding, index=False)
        print(f"Wrote {output_path}")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Flatten dossier complet data by mapping.")
    parser.add_argument("--raw", type=Path, required=True, help="Path to dossier_complet_2025.csv")
    parser.add_argument("--mapping", type=Path, required=True, help="Path to mapping_dossier_complet.csv")
    parser.add_argument("--output-dir", type=Path, required=True, help="Output directory for tables")
    parser.add_argument("--meta", type=Path, default=None, help="Optional path to meta_dossier_complet.csv")
    parser.add_argument("--dim-geo", type=Path, default=None, help="Path to dim_geographie.csv for CODGEO filtering")
    parser.add_argument("--sep", type=str, default=";", help="CSV delimiter (default: ';')")
    parser.add_argument("--encoding", type=str, default="utf-8", help="CSV encoding (default: utf-8)")
    parser.add_argument("--limit-rows", type=int, default=None, help="Optional row limit for testing")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    flatten_dossier_complet(
        raw_path=args.raw,
        mapping_path=args.mapping,
        output_dir=args.output_dir,
        meta_path=args.meta,
        dim_geo_path=args.dim_geo,
        sep=args.sep,
        encoding=args.encoding,
        limit_rows=args.limit_rows,
    )


if __name__ == "__main__":
    main()

