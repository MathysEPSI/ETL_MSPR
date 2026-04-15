### Example
Les données sont dans le dossier /data, et les sorties dans /processed_data

#### Folder (2008 & 2014 & 2020 & 2026)
```powershell
python run_all_parsers.py --input data --format csv --output processed_data/elections_flat.csv
```

#### Single file #1
```powershell
python run_all_parsers.py --input data\MN14_Bvot_T1T2.txt --format parquet --output processed_data/mn14_flat.parquet
```

#### Single file #2
```powershell
python run_all_parsers.py --input data --year 2008,2014 --format csv
python run_all_parsers.py --input data --year all --format parquet
```

#### Options
- `--input` / `--source-dir` : fichier ou dossier d’entrée
- `--output` : chemin exact du fichier de sortie
- `--format` : `csv` ou `parquet`
- `--encoding` : encodage des sources, par défaut `latin-1`
- `--year` / `--years` : `all` par défaut, ou liste séparée par des virgules
- `--strict` : échoue si un jeu de données attendu manque dans le dossier

## Outputs
Le format final contient 17 colonnes harmonisées :

- métadonnées d’élection : `annee_election`, `tour`
- géographie : `code_departement`, `libelle_departement`, `code_commune`, `libelle_commune`, `code_bureau_vote`
- agrégats bureau de vote : `inscrits`, `abstentions`, `votants`, `blancs_nuls`, `exprimes`
- ligne résultat : `code_nuance`, `nom`, `prenom`, `liste`, `voix`

Les colonnes numériques sont exportées en entier quand c’est pertinent. Les pourcentages dérivables (`pct_*`) ne sont plus exportés et peuvent être calculés côté BI.
