import argparse
from pathlib import Path

import pandas as pd

from election_etl_common import (
    DEFAULT_ENCODING,
    finalize_output_frame,
    to_int,
    write_output_frame,
)

GROUP_KEYS = [
    "tour",
    "code_departement",
    "code_commune",
    "code_bureau_vote",
]


def extract_and_process_2014(source_file: str, encoding: str = "latin-1", year: int = 2014) -> pd.DataFrame:
    records: list[dict[str, object]] = []

    with Path(source_file).open("r", encoding=encoding) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("--"):
                continue

            fields = line.split(";")
            if len(fields) < 13:
                continue

            (
                tour,
                code_dep,
                code_commune,
                libelle_commune,
                code_bureau,
                inscrits,
                votants,
                exprimes,
                numero_liste,
                nom,
                prenom,
                code_nuance,
                voix,
            ) = fields[:13]

            records.append(
                {
                    "annee_election": year,
                    "tour": tour,
                    "code_departement": code_dep,
                    "libelle_departement": pd.NA,
                    "code_commune": code_commune,
                    "libelle_commune": libelle_commune,
                    "code_bureau_vote": code_bureau,
                    "inscrits": inscrits,
                    "votants": votants,
                    "exprimes": exprimes,
                    "code_nuance": code_nuance,
                    "sexe": pd.NA,
                    "nom": nom,
                    "prenom": prenom,
                    "liste": numero_liste,
                    "voix": voix,
                }
            )

    df = pd.DataFrame(records)

    for col in ["annee_election", "tour", "inscrits", "votants", "exprimes", "voix"]:
        df[col] = to_int(df[col])

    grouped = df.groupby(GROUP_KEYS, dropna=False)
    for col in ["inscrits", "votants", "exprimes"]:
        df[col] = grouped[col].transform("max")

    df["abstentions"] = df["inscrits"] - df["votants"]
    df["blancs_nuls"] = df["votants"] - df["exprimes"]


    return finalize_output_frame(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Flatten et enrichit les resultats municipales 2014.")
    parser.add_argument("--source", default="data/MN14_Bvot_T1T2.txt", help="Fichier source .txt 2014")
    parser.add_argument("--output", default=None, help="Fichier de sortie")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Format de sortie")
    parser.add_argument("--encoding", default=DEFAULT_ENCODING, help="Encodage du fichier source")
    parser.add_argument("--year", type=int, default=2014, help="Annee election a injecter")
    args = parser.parse_args()

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path("processed_data/mn14_flat.csv" if args.format == "csv" else "processed_data/mn14_flat.parquet")

    df = extract_and_process_2014(source_file=args.source, encoding=args.encoding, year=args.year)

    write_output_frame(df, output_path, args.format, args.encoding)


if __name__ == "__main__":
    main()

