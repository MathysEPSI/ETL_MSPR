import argparse
from pathlib import Path

import pandas as pd

from .common import DEFAULT_ENCODING, finalize_output_frame, write_output_frame


def extract_and_process_2008(source_file: str, encoding: str = "latin-1", year: int = 2008) -> pd.DataFrame:
    fixed_col_count = 17
    repeating_col_count = 9

    def split_line(line: str) -> list[str]:
        parts = line.rstrip("\n\r").split(";")
        if parts and parts[-1] == "":
            parts.pop()
        return parts

    path = Path(source_file)
    lines = path.read_text(encoding=encoding).splitlines()

    try:
        tour1_idx = next(i for i, line in enumerate(lines) if line.strip() == "Tour1")
        tour2_idx = next(i for i, line in enumerate(lines) if line.strip() == "Tour2")
    except StopIteration as exc:
        raise ValueError("Marqueurs 'Tour1' et/ou 'Tour2' introuvables.") from exc

    if tour2_idx <= tour1_idx:
        raise ValueError("Ordre invalide: 'Tour2' doit etre apres 'Tour1'.")

    records: list[dict[str, object]] = []

    # Skip header line after each Tour marker: data starts at marker + 2.
    for tour, section in ((1, lines[tour1_idx + 2 : tour2_idx]), (2, lines[tour2_idx + 2 :])):
        for raw in section:
            if not raw.strip():
                continue

            fields = split_line(raw)
            if len(fields) < fixed_col_count:
                continue

            base = fields[:fixed_col_count]
            repeated = fields[fixed_col_count:]

            for i in range(0, len(repeated), repeating_col_count):
                cand = repeated[i : i + repeating_col_count]
                if len(cand) < repeating_col_count:
                    continue

                code_nuance, sexe, nom, prenom, liste, _sieges, voix, _pct_voix_ins, _pct_voix_exp = cand
                if not any(x.strip() for x in (liste, nom, voix)):
                    continue

                records.append(
                    {
                        "annee_election": year,
                        "tour": tour,
                        "code_departement": base[1],
                        "libelle_departement": base[2],
                        "code_commune": base[3],
                        "libelle_commune": base[4],
                        "code_bureau_vote": base[5],
                        "inscrits": base[6],
                        "abstentions": base[7],
                        "votants": base[9],
                        "blancs_nuls": base[11],
                        "exprimes": base[14],
                        "code_nuance": code_nuance,
                        "sexe": sexe,
                        "nom": nom,
                        "prenom": prenom,
                        "liste": liste,
                        "voix": voix,
                    }
                )

    df = pd.DataFrame(records)

    # Hard fix requested: 2008 Chatenay-Malabry (92019) has corrupted inscrits scale.
    if year == 2008 and not df.empty:
        code_dep = df["code_departement"].astype("string").str.strip()
        code_com = df["code_commune"].astype("string").str.strip()
        mask_chatenay_2008 = code_dep.eq("92") & (code_com.eq("019") | code_com.eq("92019"))
        df.loc[mask_chatenay_2008, "abstentions"] = "0"
        df.loc[mask_chatenay_2008, "inscrits"] = df.loc[mask_chatenay_2008, "votants"]

    return finalize_output_frame(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Flatten et enrichit les resultats municipales 2008.")
    parser.add_argument("--source", default="data/MN08_BVot_T1T2.txt", help="Fichier source .txt 2008")
    parser.add_argument("--output", default=None, help="Fichier de sortie")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Format de sortie")
    parser.add_argument("--encoding", default=DEFAULT_ENCODING, help="Encodage du fichier source")
    parser.add_argument("--year", type=int, default=2008, help="Annee election a injecter")
    args = parser.parse_args()

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path("processed_data/mn08_flat.csv" if args.format == "csv" else "processed_data/mn08_flat.parquet")

    df = extract_and_process_2008(source_file=args.source, encoding=args.encoding, year=args.year)

    write_output_frame(df, output_path, args.format, args.encoding)


if __name__ == "__main__":
    main()


