from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

DEFAULT_ELECTIONS_FLAT = Path("processed_data/elections_flat.csv")
DEFAULT_INSEE_2020 = Path("data/dossier_complet_2020/dossier_complet_2020.csv")
DEFAULT_INSEE_2025 = Path("data/dossier_complet_2025/dossier_complet_2025.csv")
DEFAULT_META_2025 = Path("data/dossier_complet_2025/meta_dossier_complet.csv")
DEFAULT_OUTPUT_DIR = Path("processed_data/insee")
DEFAULT_ENCODING = "latin-1"


@dataclass(frozen=True)
class YearInput:
    year: int
    path: Path


def _normalize_code_commune(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.strip()
    out = out.mask(out.isna() | out.eq(""), pd.NA)
    return out.str.upper().str.zfill(5)


def _non_empty_mask(series: pd.Series) -> pd.Series:
    values = series.astype("string")
    return values.notna() & values.str.strip().ne("")


def _read_election_communes(flat_path: Path, encoding: str) -> set[str]:
    if not flat_path.exists():
        raise FileNotFoundError(f"Fichier elections flat introuvable: {flat_path}")

    flat = pd.read_csv(flat_path, sep=";", encoding=encoding, dtype="string", usecols=["code_commune"])
    codes = _normalize_code_commune(flat["code_commune"]).dropna().unique().tolist()
    return set(codes)


def _read_insee_for_communes(path: Path, commune_codes: set[str], encoding: str, chunksize: int) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Source INSEE introuvable: {path}")

    header = pd.read_csv(path, sep=";", encoding=encoding, dtype="string", nrows=0)
    if "CODGEO" not in header.columns:
        raise ValueError(f"Colonne CODGEO absente dans: {path}")

    chunks: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, sep=";", encoding=encoding, dtype="string", chunksize=chunksize):
        chunk["CODGEO"] = _normalize_code_commune(chunk["CODGEO"])
        filtered = chunk[chunk["CODGEO"].isin(commune_codes)]
        if not filtered.empty:
            chunks.append(filtered)

    if not chunks:
        return pd.DataFrame(columns=header.columns)

    out = pd.concat(chunks, ignore_index=True)
    out = out.drop_duplicates(subset=["CODGEO"], keep="first").reset_index(drop=True)
    return out


def _build_column_profile(df: pd.DataFrame, threshold: float, year: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    row_count = len(df)

    for column in df.columns:
        if column == "CODGEO":
            continue

        non_empty = int(_non_empty_mask(df[column]).sum())
        ratio = (non_empty / row_count) if row_count else 0.0
        rows.append(
            {
                "snapshot_year": year,
                "indicateur_code": column,
                "non_null_count": non_empty,
                "row_count": row_count,
                "non_null_ratio": ratio,
                "keep": ratio >= threshold,
            }
        )

    return pd.DataFrame(rows).sort_values(["keep", "non_null_ratio", "indicateur_code"], ascending=[False, False, True])


def _read_meta_2025(path: Path, encoding: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Metadata INSEE 2025 introuvable: {path}")

    meta = pd.read_csv(
        path,
        sep=";",
        encoding=encoding,
        dtype="string",
        header=None,
        names=[
            "code_variable",
            "libelle_variable",
            "description",
            "valeur_exemple",
            "libelle_exemple",
            "type",
            "longueur",
            "decimales",
            "source",
        ],
    )

    meta = meta[meta["code_variable"].notna() & meta["code_variable"].ne("CODGEO")].copy()
    meta = meta.drop_duplicates(subset=["code_variable"], keep="first")
    return meta


def _build_indicator_dimension(
    kept_2020: list[str],
    kept_2025: list[str],
    meta_2025: pd.DataFrame,
    threshold: float,
) -> pd.DataFrame:
    codes = sorted(set(kept_2020) | set(kept_2025))
    meta_map = meta_2025.set_index("code_variable")

    rows: list[dict[str, object]] = []
    for code in codes:
        in_meta = code in meta_map.index
        meta_row = meta_map.loc[code] if in_meta else None
        rows.append(
            {
                "indicateur_code": code,
                "libelle_variable": (meta_row["libelle_variable"] if in_meta else pd.NA),
                "description": (meta_row["description"] if in_meta else pd.NA),
                "type_source": (meta_row["type"] if in_meta else pd.NA),
                "source": (meta_row["source"] if in_meta else pd.NA),
                "metadata_status": "meta_2025" if in_meta else "header_only",
                "available_2020": code in kept_2020,
                "available_2025": code in kept_2025,
                "selection_threshold": threshold,
            }
        )

    return pd.DataFrame(rows)


def _build_fact_long(df_by_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []

    for year, frame in df_by_year.items():
        indicators = [col for col in frame.columns if col != "CODGEO"]
        if not indicators:
            continue

        melted = frame.melt(
            id_vars=["CODGEO"],
            value_vars=indicators,
            var_name="indicateur_code",
            value_name="valeur_raw",
        )

        non_empty = _non_empty_mask(melted["valeur_raw"])
        melted = melted[non_empty].copy()
        if melted.empty:
            continue

        melted = melted.rename(columns={"CODGEO": "code_commune"})
        melted.insert(0, "snapshot_year", year)
        numeric = melted["valeur_raw"].astype("string").str.replace(",", ".", regex=False)
        melted["valeur_num"] = pd.to_numeric(numeric, errors="coerce")
        parts.append(melted)

    if not parts:
        return pd.DataFrame(columns=["fact_insee_id", "snapshot_year", "code_commune", "indicateur_code", "valeur_raw", "valeur_num"])

    fact = pd.concat(parts, ignore_index=True)
    fact.insert(0, "fact_insee_id", pd.RangeIndex(start=1, stop=len(fact) + 1, step=1))
    return fact


def _build_election_insee_bridge(elections_flat: Path, encoding: str) -> pd.DataFrame:
    flat = pd.read_csv(elections_flat, sep=";", encoding=encoding, dtype="string", usecols=["annee_election"])
    years_numeric = pd.Series(pd.to_numeric(flat["annee_election"], errors="coerce"), dtype="float64")
    years = sorted(years_numeric.dropna().astype(int).unique().tolist())

    rows: list[dict[str, int]] = []
    for year in years:
        # Regle simple pour BI: 2020 pour 2008/2014/2020, 2025 pour les annees >= 2025.
        snapshot_year = 2025 if year >= 2025 else 2020
        rows.append({"annee_election": year, "snapshot_year": snapshot_year})

    return pd.DataFrame(rows)


def _write_table(df: pd.DataFrame, path_no_suffix: Path, file_format: str, encoding: str) -> Path:
    path_no_suffix.parent.mkdir(parents=True, exist_ok=True)
    if file_format == "parquet":
        out_path = path_no_suffix.with_suffix(".parquet")
        df.to_parquet(out_path, index=False)
        return out_path

    out_path = path_no_suffix.with_suffix(".csv")
    df.to_csv(out_path, sep=";", encoding=encoding, index=False)
    return out_path


def run_insee_pipeline(
    elections_flat: Path,
    source_2020: Path,
    source_2025: Path,
    meta_2025: Path,
    output_dir: Path,
    threshold: float,
    file_format: str,
    encoding: str,
    chunksize: int,
) -> dict[str, Path]:
    if not 0.0 <= threshold <= 1.0:
        raise ValueError("--threshold doit etre entre 0 et 1")

    election_communes = _read_election_communes(elections_flat, encoding)
    year_inputs = [YearInput(2020, source_2020), YearInput(2025, source_2025)]

    filtered_year_frames: dict[int, pd.DataFrame] = {}
    profiles: list[pd.DataFrame] = []
    kept_columns_by_year: dict[int, list[str]] = {}

    for year_input in year_inputs:
        raw = _read_insee_for_communes(year_input.path, election_communes, encoding=encoding, chunksize=chunksize)
        profile = _build_column_profile(raw, threshold=threshold, year=year_input.year)
        profiles.append(profile)

        # Keep all indicators from each source year (no column drop at ETL stage).
        kept_columns = [col for col in raw.columns if col != "CODGEO"]
        kept_columns_by_year[year_input.year] = kept_columns

        final_columns = ["CODGEO", *kept_columns]
        filtered_year_frames[year_input.year] = raw[final_columns].rename(columns={"CODGEO": "code_commune"})

    meta = _read_meta_2025(meta_2025, encoding=encoding)
    dim_indicator = _build_indicator_dimension(
        kept_2020=kept_columns_by_year.get(2020, []),
        kept_2025=kept_columns_by_year.get(2025, []),
        meta_2025=meta,
        threshold=threshold,
    )

    common_codes = sorted(
        set(kept_columns_by_year.get(2020, []))
        & set(kept_columns_by_year.get(2025, []))
    )

    fact_source = {
        year: df.rename(columns={"code_commune": "CODGEO"})
        for year, df in filtered_year_frames.items()
    }
    fact_insee = _build_fact_long(fact_source)
    fact_insee_common = fact_insee[fact_insee["indicateur_code"].isin(common_codes)].copy()

    dim_indicator_common = dim_indicator[
        dim_indicator["available_2020"] & dim_indicator["available_2025"]
    ].copy()

    bridge_election_insee = _build_election_insee_bridge(elections_flat, encoding=encoding)

    coverage = pd.DataFrame(
        [
            {
                "snapshot_year": 2020,
                "election_communes": len(election_communes),
                "matched_communes": int(filtered_year_frames[2020]["code_commune"].nunique()),
                "missing_communes": int(len(election_communes) - filtered_year_frames[2020]["code_commune"].nunique()),
                "kept_indicators": len(kept_columns_by_year[2020]),
            },
            {
                "snapshot_year": 2025,
                "election_communes": len(election_communes),
                "matched_communes": int(filtered_year_frames[2025]["code_commune"].nunique()),
                "missing_communes": int(len(election_communes) - filtered_year_frames[2025]["code_commune"].nunique()),
                "kept_indicators": len(kept_columns_by_year[2025]),
            },
        ]
    )

    output_tables = output_dir / "tables"
    output_reports = output_dir / "reports"

    outputs: dict[str, Path] = {}
    outputs["insee_2020_wide"] = _write_table(filtered_year_frames[2020], output_tables / "insee_2020_commune_selected_wide", file_format, encoding)
    outputs["insee_2025_wide"] = _write_table(filtered_year_frames[2025], output_tables / "insee_2025_commune_selected_wide", file_format, encoding)
    outputs["fact_insee"] = _write_table(fact_insee, output_tables / "fact_insee_commune_snapshot", file_format, encoding)
    outputs["fact_insee_common"] = _write_table(
        fact_insee_common,
        output_tables / "fact_insee_commune_snapshot_common",
        file_format,
        encoding,
    )

    dim_path = output_tables / "dim_insee_indicateur.csv"
    dim_path.parent.mkdir(parents=True, exist_ok=True)
    dim_indicator.to_csv(dim_path, sep=";", encoding=encoding, index=False)
    outputs["dim_insee_indicateur"] = dim_path

    dim_common_path = output_tables / "dim_insee_indicateur_common.csv"
    dim_indicator_common.to_csv(dim_common_path, sep=";", encoding=encoding, index=False)
    outputs["dim_insee_indicateur_common"] = dim_common_path

    bridge_path = output_tables / "bridge_election_insee_snapshot.csv"
    bridge_election_insee.to_csv(bridge_path, sep=";", encoding=encoding, index=False)
    outputs["bridge_election_insee_snapshot"] = bridge_path

    profile_df = pd.concat(profiles, ignore_index=True)
    coverage_path = output_reports / "coverage_report.csv"
    coverage_path.parent.mkdir(parents=True, exist_ok=True)
    coverage.to_csv(coverage_path, sep=";", encoding=encoding, index=False)
    outputs["coverage_report"] = coverage_path

    profile_path = output_reports / "column_profile_report.csv"
    profile_df.to_csv(profile_path, sep=";", encoding=encoding, index=False)
    outputs["column_profile_report"] = profile_path

    manifest = {
        "threshold": threshold,
        "file_format": file_format,
        "encoding": encoding,
        "elections_flat": str(elections_flat),
        "source_2020": str(source_2020),
        "source_2025": str(source_2025),
        "meta_2025": str(meta_2025),
        "election_communes": len(election_communes),
        "rows_2020_after_filter": int(len(filtered_year_frames[2020])),
        "rows_2025_after_filter": int(len(filtered_year_frames[2025])),
        "kept_indicators_2020": len(kept_columns_by_year[2020]),
        "kept_indicators_2025": len(kept_columns_by_year[2025]),
        "kept_indicators_common": len(common_codes),
        "fact_rows": int(len(fact_insee)),
        "fact_rows_common": int(len(fact_insee_common)),
    }
    manifest_path = output_reports / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    outputs["run_manifest"] = manifest_path

    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline INSEE: filtre les communes du flat, conserve les indicateurs >= seuil de non-null, et exporte des tables BI."
    )
    parser.add_argument("--elections-flat", default=str(DEFAULT_ELECTIONS_FLAT), help="Fichier elections_flat.csv")
    parser.add_argument("--source-2020", default=str(DEFAULT_INSEE_2020), help="Source INSEE 2020")
    parser.add_argument("--source-2025", default=str(DEFAULT_INSEE_2025), help="Source INSEE 2025")
    parser.add_argument("--meta-2025", default=str(DEFAULT_META_2025), help="Metadata INSEE 2025")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Dossier de sortie")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.30,
        help="Seuil non-null utilise pour le reporting de qualite (0-1), sans suppression de colonnes",
    )
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet", help="Format de sortie principal")
    parser.add_argument("--encoding", default=DEFAULT_ENCODING, help="Encodage des CSV")
    parser.add_argument("--chunksize", type=int, default=5000, help="Taille des chunks de lecture INSEE")
    args = parser.parse_args()

    outputs = run_insee_pipeline(
        elections_flat=Path(args.elections_flat),
        source_2020=Path(args.source_2020),
        source_2025=Path(args.source_2025),
        meta_2025=Path(args.meta_2025),
        output_dir=Path(args.output_dir),
        threshold=args.threshold,
        file_format=args.format,
        encoding=args.encoding,
        chunksize=args.chunksize,
    )

    print("[DONE] Pipeline INSEE termine.")
    for key, output in outputs.items():
        print(f"- {key}: {output}")


if __name__ == "__main__":
    main()



