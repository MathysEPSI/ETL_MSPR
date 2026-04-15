import argparse
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
REPEATING_COL_COUNT = 9


def _parse_file(path: Path, tour: int, sep: str, encoding: str, year: int) -> list[dict[str, object]]:
	records: list[dict[str, object]] = []

	with path.open("r", encoding=encoding) as f:
		next(f, None)  # Skip header
		for raw in f:
			line = raw.rstrip("\n\r")
			if not line:
				continue

			fields = line.split(sep)
			while fields and fields[-1] == "":
				fields.pop()

			if len(fields) < BASE_COL_COUNT:
				continue

			base = fields[:BASE_COL_COUNT]
			repeated = fields[BASE_COL_COUNT:]

			for i in range(0, len(repeated), REPEATING_COL_COUNT):
				candidate = repeated[i : i + REPEATING_COL_COUNT]
				if len(candidate) < REPEATING_COL_COUNT:
					continue

				_npan, code_nuance, sexe, nom, prenom, liste, voix, pct_voix_ins, pct_voix_exp = candidate
				if not any(x.strip() for x in (code_nuance, nom, prenom, liste, voix)):
					continue

				records.append(
					{
						"annee_election": year,
						"tour": tour,
						"code_departement": base[0],
						"libelle_departement": base[1],
						"code_commune": base[2],
						"libelle_commune": base[3],
						"code_bureau_vote": base[4],
						"inscrits": base[5],
						"abstentions": base[6],
						"votants": base[8],
						"blancs": base[10],
						"nuls": base[13],
						"exprimes": base[16],
						"code_nuance": code_nuance,
						"sexe": sexe,
						"nom": nom,
						"prenom": prenom,
						"liste": liste,
						"voix": voix,
						"pct_voix_ins": pct_voix_ins,
						"pct_voix_exp": pct_voix_exp,
					}
				)

	return records


def extract_and_process_2020(source_t1: str, source_t2: str, encoding: str = "latin-1", year: int = 2020) -> pd.DataFrame:
	records: list[dict[str, object]] = []
	records.extend(_parse_file(Path(source_t1), tour=1, sep="\t", encoding=encoding, year=year))
	records.extend(_parse_file(Path(source_t2), tour=2, sep=";", encoding=encoding, year=year))

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
	df["code_commune"] = normalize_code(df["code_commune"], width=3)
	df["code_bureau_vote"] = normalize_code(df["code_bureau_vote"], width=4)

	return finalize_output_frame(df)


def main() -> None:
	parser = argparse.ArgumentParser(description="Flatten et harmonise les resultats municipales 2020.")
	parser.add_argument(
		"--source-t1",
		default="data/2020-05-18-resultats-par-niveau-burvot-t1-france-entiere.txt",
		help="Fichier source tour 1",
	)
	parser.add_argument(
		"--source-t2",
		default="data/resultats-par-niveau-burvot-t2-france-entiere.txt",
		help="Fichier source tour 2",
	)
	parser.add_argument("--output", default=None, help="Fichier de sortie")
	parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Format de sortie")
	parser.add_argument("--encoding", default=DEFAULT_ENCODING, help="Encodage des fichiers source")
	parser.add_argument("--year", type=int, default=2020, help="Annee election a injecter")
	args = parser.parse_args()

	if args.output:
		output_path = Path(args.output)
	else:
		output_path = Path("processed_data/mn20_flat.csv" if args.format == "csv" else "processed_data/mn20_flat.parquet")

	df = extract_and_process_2020(
		source_t1=args.source_t1,
		source_t2=args.source_t2,
		encoding=args.encoding,
		year=args.year,
	)

	write_output_frame(df, output_path, args.format, args.encoding)


if __name__ == "__main__":
	main()


