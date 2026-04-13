import argparse
import csv
from pathlib import Path

import pandas as pd

from election_etl_common import (
    DEFAULT_ENCODING,
    finalize_output_frame,
    normalize_code,
    percent,
    to_int,
    write_output_frame,
)

BASE_COL_COUNT = 19
CANDIDATE_COL_COUNT = 13


def _clean(value: str) -> str | None:
    text = value.strip()
    return text if text else None


def _extract_records(path: Path, tour: int, encoding: str, year: int) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []

    with path.open("r", encoding=encoding, newline="") as fh:
        reader = csv.reader(fh, delimiter=";", quotechar='"')
        header = next(reader, None)
        if not header:
            return records

        candidate_slots = max((len(header) - BASE_COL_COUNT) // CANDIDATE_COL_COUNT, 0)

        for row in reader:
            if not row:
                continue
            if len(row) < BASE_COL_COUNT:
                row = row + [""] * (BASE_COL_COUNT - len(row))

            base = {
                "annee_election": year,
                "tour": tour,
                "code_departement": _clean(row[0]),
                "libelle_departement": _clean(row[1]),
                "code_commune": _clean(row[2]),
                "libelle_commune": _clean(row[3]),
                "code_bureau_vote": _clean(row[4]),
                "inscrits": _clean(row[5]),
                "votants": _clean(row[6]),
                "abstentions": _clean(row[8]),
                "exprimes": _clean(row[10]),
                "blancs": _clean(row[13]),
                "nuls": _clean(row[16]),
            }

            for slot in range(candidate_slots):
                offset = BASE_COL_COUNT + slot * CANDIDATE_COL_COUNT
                block = row[offset : offset + CANDIDATE_COL_COUNT]
                if len(block) < CANDIDATE_COL_COUNT:
                    continue

                nom = _clean(block[1])
                prenom = _clean(block[2])
                sexe = _clean(block[3])
                code_nuance = _clean(block[4])
                liste_abbr = _clean(block[5])
                liste_full = _clean(block[6])
                voix = _clean(block[7])

                liste = liste_full if liste_full else liste_abbr
                if not any((code_nuance, nom, prenom, liste, voix)):
                    continue

                records.append(
                    {
                        **base,
                        "code_nuance": code_nuance,
                        "sexe": sexe,
                        "nom": nom,
                        "prenom": prenom,
                        "liste": liste,
                        "voix": voix,
                    }
                )

    return records


def extract_and_process_2026(source_t1: str, source_t2: str, encoding: str = "latin-1", year: int = 2026) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    records.extend(_extract_records(Path(source_t1), tour=1, encoding=encoding, year=year))
    records.extend(_extract_records(Path(source_t2), tour=2, encoding=encoding, year=year))

    df = pd.DataFrame(records)
    if df.empty:
        return finalize_output_frame(pd.DataFrame())

    for col in ["inscrits", "abstentions", "votants", "blancs", "nuls", "exprimes", "voix"]:
        df[col] = to_int(df[col])

    df["blancs_nuls"] = df["blancs"] + df["nuls"]

    df["pct_abs_ins"] = percent(df["abstentions"], df["inscrits"])
    df["pct_vot_ins"] = percent(df["votants"], df["inscrits"])
    df["pct_blnuls_ins"] = percent(df["blancs_nuls"], df["inscrits"])
    df["pct_blnuls_vot"] = percent(df["blancs_nuls"], df["votants"])
    df["pct_exp_ins"] = percent(df["exprimes"], df["inscrits"])
    df["pct_exp_vot"] = percent(df["exprimes"], df["votants"])
    df["pct_voix_ins"] = percent(df["voix"], df["inscrits"])
    df["pct_voix_exp"] = percent(df["voix"], df["exprimes"])

    df["code_departement"] = normalize_code(df["code_departement"], width=2)
    df["code_commune"] = normalize_code(df["code_commune"], width=5)
    df["code_bureau_vote"] = normalize_code(df["code_bureau_vote"], width=4)

    return finalize_output_frame(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Flatten et harmonise les resultats municipales 2026.")
    parser.add_argument(
        "--source-t1",
        default="data/municipales-2026-resultats-bv-par-communes-2026-03-20.csv",
        help="Fichier source tour 1",
    )
    parser.add_argument(
        "--source-t2",
        default="data/municipales-2026-resultats-bureau-de-vote-2026-03-23-16h15.csv",
        help="Fichier source tour 2",
    )
    parser.add_argument("--output", default=None, help="Fichier de sortie")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Format de sortie")
    parser.add_argument("--encoding", default=DEFAULT_ENCODING, help="Encodage des fichiers source")
    parser.add_argument("--year", type=int, default=2026, help="Annee election a injecter")
    args = parser.parse_args()

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path("processed_data/mn26_flat.csv" if args.format == "csv" else "processed_data/mn26_flat.parquet")

    df = extract_and_process_2026(
        source_t1=args.source_t1,
        source_t2=args.source_t2,
        encoding=args.encoding,
        year=args.year,
    )

    write_output_frame(df, output_path, args.format, args.encoding)


if __name__ == "__main__":
    main()

