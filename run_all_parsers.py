from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from src.preprocessing.election_etl_common import DEFAULT_ENCODING, DEFAULT_OUTPUT_STEM, finalize_output_frame, write_output_frame
from src.preprocessing.extract_and_transform_2008 import extract_and_process_2008
from src.preprocessing.extract_and_transform_2014 import extract_and_process_2014
from src.preprocessing.extract_and_transform_2020 import extract_and_process_2020
from src.preprocessing.extract_and_transform_2026 import extract_and_process_2026


@dataclass(frozen=True)
class YearSpec:
    year: int
    source_names: tuple[str, ...]
    loader: Callable[[list[Path], str, int], pd.DataFrame]


def _load_2008(paths: list[Path], encoding: str, year: int) -> pd.DataFrame:
    return extract_and_process_2008(source_file=str(paths[0]), encoding=encoding, year=year)


def _load_2014(paths: list[Path], encoding: str, year: int) -> pd.DataFrame:
    return extract_and_process_2014(source_file=str(paths[0]), encoding=encoding, year=year)


def _load_2020(paths: list[Path], encoding: str, year: int) -> pd.DataFrame:
    return extract_and_process_2020(source_t1=str(paths[0]), source_t2=str(paths[1]), encoding=encoding, year=year)


def _load_2026(paths: list[Path], encoding: str, year: int) -> pd.DataFrame:
    return extract_and_process_2026(source_t1=str(paths[0]), source_t2=str(paths[1]), encoding=encoding, year=year)


YEAR_SPECS: tuple[YearSpec, ...] = (
    YearSpec(year=2008, source_names=("MN08_BVot_T1T2.txt",), loader=_load_2008),
    YearSpec(year=2014, source_names=("MN14_Bvot_T1T2.txt",), loader=_load_2014),
    YearSpec(
        year=2020,
        source_names=(
            "2020-05-18-resultats-par-niveau-burvot-t1-france-entiere.txt",
            "resultats-par-niveau-burvot-t2-france-entiere.txt",
        ),
        loader=_load_2020,
    ),
    YearSpec(
        year=2026,
        source_names=(
            "municipales-2026-resultats-bv-par-communes-2026-03-20.csv",
            "municipales-2026-resultats-bureau-de-vote-2026-03-23-16h15.csv",
        ),
        loader=_load_2026,
    ),
)


def parse_years_arg(raw_years: str) -> list[int]:
    available_years = [spec.year for spec in YEAR_SPECS]
    if raw_years.strip().lower() == "all":
        return available_years

    years: list[int] = []
    for token in raw_years.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            year = int(token)
        except ValueError as exc:
            raise ValueError(f"Annee invalide dans --year: {token}") from exc
        if year not in available_years:
            valid = ", ".join(str(y) for y in available_years)
            raise ValueError(f"Annee non supportee: {year}. Valeurs possibles: {valid}")
        years.append(year)

    if not years:
        raise ValueError("--year est vide. Exemple: --year 2008,2014 ou --year all")

    seen: set[int] = set()
    ordered_unique: list[int] = []
    for year in years:
        if year not in seen:
            ordered_unique.append(year)
            seen.add(year)
    return ordered_unique


def find_spec_by_filename(filename: str) -> YearSpec | None:
    for spec in YEAR_SPECS:
        if filename in spec.source_names:
            return spec
    return None


def resolve_sources(spec: YearSpec, input_path: Path) -> list[Path] | None:
    if input_path.is_file():
        if input_path.name not in spec.source_names:
            return None
        if len(spec.source_names) == 1:
            return [input_path]

        sources = [input_path.with_name(name) for name in spec.source_names]
        if all(path.exists() for path in sources):
            return sources
        return None

    if input_path.is_dir():
        sources = [input_path / name for name in spec.source_names]
        if all(path.exists() for path in sources):
            return sources

    return None


def build_output_path(output: str | None, output_dir: str | None, file_format: str) -> Path:
    if output:
        return Path(output)
    if output_dir:
        return Path(output_dir) / f"{DEFAULT_OUTPUT_STEM}.{file_format}"
    return Path("processed_data") / f"{DEFAULT_OUTPUT_STEM}.{file_format}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL general: traite un fichier ou un dossier et concatene les annees disponibles."
    )
    parser.add_argument(
        "--input",
        "--source-dir",
        dest="input_path",
        default="data",
        help="Fichier source unique ou dossier contenant les fichiers sources",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Chemin du fichier de sortie unique. Si absent, un nom par defaut est utilise.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Dossier de sortie legacy; utilise le nom de fichier par defaut a l'interieur.",
    )
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Format de sortie")
    parser.add_argument("--encoding", default=DEFAULT_ENCODING, help="Encodage des fichiers source")
    parser.add_argument(
        "--year",
        "--years",
        dest="year",
        default="all",
        help="Annee(s) a traiter: all (defaut) ou liste separee par des virgules (ex: 2008,2014)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Echoue si un jeu de donnees attendu est absent",
    )
    args = parser.parse_args()

    input_path = Path(args.input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Entree introuvable: {input_path}")

    selected_years = parse_years_arg(args.year)
    specs_by_year = {spec.year: spec for spec in YEAR_SPECS}
    output_path = build_output_path(args.output, args.output_dir, args.format)

    if input_path.is_file():
        matching_spec = find_spec_by_filename(input_path.name)
        if matching_spec is None:
            raise FileNotFoundError(f"Aucun parser ne correspond au fichier: {input_path.name}")

        if matching_spec.year not in selected_years:
            valid = ", ".join(str(y) for y in selected_years)
            raise ValueError(f"Le fichier fourni correspond a {matching_spec.year}, mais --year vaut: {valid}")

        sources = resolve_sources(matching_spec, input_path)
        if sources is None:
            raise FileNotFoundError(f"Fichier(s) source incomplet(s) pour {matching_spec.year} autour de: {input_path}")

        print(f"[RUN] {matching_spec.year} -> {output_path}")
        df = matching_spec.loader(sources, args.encoding, matching_spec.year)
        df = finalize_output_frame(df)
        write_output_frame(df, output_path, args.format, args.encoding)
        print(f"[DONE] 1 jeu de donnees traite: {len(df)} lignes.")
        return

    frames: list[pd.DataFrame] = []
    processed_years: list[int] = []

    for year in selected_years:
        spec = specs_by_year[year]
        sources = resolve_sources(spec, input_path)
        if sources is None:
            message = f"[WARN] Fichier(s) source absent(s) pour {year} dans {input_path}"
            if args.strict:
                raise FileNotFoundError(message)
            print(message)
            continue

        print(f"[RUN] {year} -> {output_path}")
        frame = spec.loader(sources, args.encoding, year)
        frames.append(frame)
        processed_years.append(year)

    if not frames:
        raise RuntimeError("Aucun fichier source compatible detecte.")

    combined = finalize_output_frame(pd.concat(frames, ignore_index=True))
    write_output_frame(combined, output_path, args.format, args.encoding)

    years_text = ", ".join(str(year) for year in processed_years)
    print(f"[DONE] Annees traitees: {years_text}. {len(combined)} lignes exportees vers {output_path}")


if __name__ == "__main__":
    main()
