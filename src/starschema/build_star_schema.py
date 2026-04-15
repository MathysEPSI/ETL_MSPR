from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .star_schema import build_star_schema, export_tables_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Build star schema tables from elections_flat output.")
    parser.add_argument("--input", default="processed_data/elections_flat.csv", help="Flat source file produced by ETL")
    parser.add_argument("--sep", default=";", help="Input separator for flat CSV")
    parser.add_argument("--encoding", default="latin-1", help="Input encoding for flat CSV")
    parser.add_argument("--output-dir", default="processed_data/star_schema", help="Output directory for star schema tables")
    parser.add_argument(
        "--include-unknown-members",
        action="store_true",
        help="Include UNKNOWN rows (sk=0) in dimensions and map unresolved facts to 0",
    )
    parser.add_argument(
        "--export",
        choices=["csv", "none", "both"],
        default="csv",
        help="Export mode: csv writes files, none keeps in-memory build only, both aliases csv for CLI",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}")

    flat_df = pd.read_csv(input_path, sep=args.sep, encoding=args.encoding, dtype="string")
    tables = build_star_schema(flat_df, include_unknown_members=args.include_unknown_members)

    if args.export in {"csv", "both"}:
        export_tables_csv(tables, output_dir=args.output_dir, sep=";", encoding="utf-8")

    print("[DONE] Star schema built:")
    for name, table in tables.items():
        print(f"- {name}: {len(table)} rows")


if __name__ == "__main__":
    main()



