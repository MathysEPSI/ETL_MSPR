### Example
Les données sont dans le dossier /data, et les sorties dans /processed_data

#### Folder (2008 & 2014 & 2020 & 2026)
```powershell
python -m src.preprocessing.run_pipeline --input data --format csv --output processed_data/elections_flat.csv
```

#### Single file #1
```powershell
python -m src.preprocessing.run_pipeline --input data\MN14_Bvot_T1T2.txt --format parquet --output processed_data/mn14_flat.parquet
```

#### Single file #2
```powershell
python -m src.preprocessing.run_pipeline --input data --year 2008,2014 --format csv
python -m src.preprocessing.run_pipeline --input data --year all --format parquet
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

## Tables de fait & dimensions
Le fichier `processed_data/elections_flat.csv` peut être transformé en modèle en étoile.

Tables générées :
- `dim_geographie`
- `dim_election`
- `dim_bureau_vote`
- `dim_candidat_liste`
- `fact_resultats_votes`

Grain de la fact : 1 ligne = 1 candidat dans 1 bureau de vote, pour 1 tour et 1 annee.

### Export CSV des tables du modele en etoile
```powershell
python -m src.starschema.build_star_schema --input processed_data/elections_flat.csv --output-dir processed_data/star_schema --export csv
```

### Usage en DataFrames pandas (insertion BDD directe)
```python
import pandas as pd

from src.starschema.star_schema import build_star_schema, export_tables_dataframes

flat = pd.read_csv("processed_data/elections_flat.csv", sep=";", dtype="string")
tables = build_star_schema(flat)
tables_df = export_tables_dataframes(tables)
```

### Ajouter d'autres datasets analytiques (cle geographique)
Le module expose `register_geo_metrics_dataset(...)` pour rattacher un autre dataset via:
- `code_commune` (prioritaire)
- ou `code_postal + nom ville`
- ou `nom ville` seul (fallback)

Exemple minimal:
```python
import pandas as pd

from src.starschema.star_schema import GeoJoinConfig, build_star_schema, register_geo_metrics_dataset

flat = pd.read_csv("processed_data/elections_flat.csv", sep=";", dtype="string")
tables = build_star_schema(flat)

dataset = pd.DataFrame(
	{
		"code_commune": ["01004", "01007"],
		"indice_socio": [72.3, 54.1],
	}
)

fact_metrics = register_geo_metrics_dataset(
	dataset_name="indicateurs_territoriaux",
	metrics_df=dataset,
	dim_geographie=tables["dim_geographie"],
	metric_columns=["indice_socio"],
	join_config=GeoJoinConfig(code_commune_col="code_commune"),
)
```

