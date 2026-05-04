from __future__ import annotations

from pathlib import Path

from flatten_dossier_complet import flatten_dossier_complet


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    data_dir = repo_root / "data" / "dossier_complet_2025"
    output_dir = repo_root / "processed_data" / "insee" / "dossier_complet_2025_flattened"
    dim_geo_path = repo_root / "processed_data" / "star_schema" / "dim_geographie.csv"
    mapping_path = repo_root / "src" / "insee_processing" / "mapping_dossier_complet.csv"

    flatten_dossier_complet(
        raw_path=data_dir / "dossier_complet_2025.csv",
        mapping_path=mapping_path,
        meta_path=data_dir / "meta_dossier_complet.csv",
        dim_geo_path=dim_geo_path,
        output_dir=output_dir,
        sep=";",
        encoding="utf-8",
        limit_rows=None,
    )


if __name__ == "__main__":
    main()
