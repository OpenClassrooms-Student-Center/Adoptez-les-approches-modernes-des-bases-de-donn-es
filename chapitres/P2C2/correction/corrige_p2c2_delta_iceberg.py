from __future__ import annotations

from pathlib import Path
import shutil

import pandas as pd
import pyarrow as pa

from deltalake import DeltaTable, write_deltalake

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import TableAlreadyExistsError


# -----------------------------
# Helpers
# -----------------------------
def script_dir() -> Path:
    return Path(__file__).parent


def read_clients_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable: {csv_path}\n"
            "👉 Vérifie que clients.csv est bien dans le même dossier que le script."
        )
    return pd.read_csv(csv_path)


def print_title(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


# -----------------------------
# 0) Chargement du dataset
# -----------------------------
BASE = script_dir()
CSV_PATH = BASE / "clients.csv"

df_clients = read_clients_csv(CSV_PATH)

required_cols = {"id", "name", "city", "country"}
if not required_cols.issubset(set(df_clients.columns)):
    raise ValueError(
        "Le fichier clients.csv doit contenir les colonnes: id, name, city, country.\n"
        f"Colonnes trouvées: {list(df_clients.columns)}"
    )

# Ordre canonique
df_clients = df_clients[["id", "name", "city", "country"]]

print_title("Dataset clients.csv (aperçu)")
print(df_clients.head())

# Deux nouveaux clients (mêmes ajouts pour Delta + Iceberg)
df_new = pd.DataFrame(
    {
        "id": [9991, 9992],
        "name": ["Nouveau Client 1", "Nouveau Client 2"],
        "city": ["Marseille", "Toulouse"],
        "country": ["France", "France"],
    }
)[["id", "name", "city", "country"]]


# =============================
# PARTIE 1 — DELTA LAKE
# =============================
print_title("1) Delta Lake : créer la table locale à partir de clients.csv")

delta_path = BASE / "delta_clients"

# Rejouable : on repart de zéro
if delta_path.exists():
    shutil.rmtree(delta_path)

write_deltalake(str(delta_path), df_clients, mode="overwrite")
dt = DeltaTable(str(delta_path))
print(f"✅ Table Delta créée dans: {delta_path.resolve()}")
print("Version actuelle (attendue: 0) :", dt.version())

print_title("2) Delta Lake : ajouter 2 clients, puis vérifier la version")
write_deltalake(str(delta_path), df_new, mode="append")
dt = DeltaTable(str(delta_path))
print("✅ Données ajoutées.")
print("Version actuelle (attendue: 1) :", dt.version())
print("\nTable Delta (dernières lignes) :")
print(dt.to_pandas().tail())

print_title("3) Delta Lake : time travel (relire la version 0)")
dt_v0 = DeltaTable(str(delta_path), version=0)
print("✅ Lecture de la version 0.")
print(dt_v0.to_pandas().tail())

print("\n💡 Différence de taille :")
print("Version 0 - nb lignes :", len(dt_v0.to_pandas()))
print("Dernière version - nb lignes :", len(dt.to_pandas()))


# =============================
# PARTIE 2 — ICEBERG
# =============================
print_title("4) Iceberg : créer une table équivalente et charger le dataset")

warehouse_path = BASE / "iceberg_demo"
warehouse_path.mkdir(parents=True, exist_ok=True)

catalog_db = BASE / "iceberg_catalog.db"

catalog = load_catalog(
    "local",
    **{
        "type": "sql",
        "uri": f"sqlite:///{catalog_db.resolve()}",
        "warehouse": warehouse_path.resolve().as_uri(),
    },
)

# Namespace
try:
    catalog.create_namespace("default")
except Exception:
    pass

identifier = "default.customers"

# Schéma Iceberg équivalent (4 colonnes)
# Fix 1 : required=False pour éviter les soucis nullable/required avec PyArrow
schema = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "city", StringType(), required=False),
    NestedField(4, "country", StringType(), required=False),
)

try:
    table = catalog.create_table(identifier, schema=schema)
    print("✅ Table Iceberg créée :", identifier)
except TableAlreadyExistsError:
    table = catalog.load_table(identifier)
    print("✅ Table Iceberg déjà existante, chargée :", identifier)

print("Location :", table.location())

# --- Append #1 : dataset initial ---
arrow_initial = pa.table(
    {
        "id": df_clients["id"].tolist(),
        "name": df_clients["name"].tolist(),
        "city": df_clients["city"].tolist(),
        "country": df_clients["country"].tolist(),
    }
)
table.append(arrow_initial)
print("✅ Iceberg : dataset initial ajouté (snapshot créé).")

snapshots_after_initial = list(table.snapshots())
print("\nSnapshots après append #1 :")
for s in snapshots_after_initial:
    print(f"- snapshot_id={s.snapshot_id}  timestamp_ms={s.timestamp_ms}")

# --- Append #2 : les 2 nouveaux clients (même ajout que Delta) ---
arrow_new = pa.table(
    {
        "id": df_new["id"].tolist(),
        "name": df_new["name"].tolist(),
        "city": df_new["city"].tolist(),
        "country": df_new["country"].tolist(),
    }
)
table.append(arrow_new)
print("\n✅ Iceberg : 2 nouveaux clients ajoutés (nouveau snapshot créé).")

snapshots_after_new = list(table.snapshots())
print("\nSnapshots après append #2 :")
for s in snapshots_after_new:
    print(f"- snapshot_id={s.snapshot_id}  timestamp_ms={s.timestamp_ms}")

# Lecture Iceberg
print("\nLecture Iceberg (aperçu) :")
for row in table.scan().to_arrow().to_pylist()[:10]:
    print(row)

print_title("5) Comparaison rapide : Delta versions vs Iceberg snapshots")
print(f"Delta - version actuelle : {dt.version()} (on attend 1)")
print(f"Iceberg - nb snapshots : {len(snapshots_after_new)} (on attend 2+ selon création/état)")

print("\n✅ Exercice terminé.")
print(f"📁 Delta:   {delta_path.resolve()}")
print(f"📁 Iceberg: {warehouse_path.resolve()}")

print("\nStructure Iceberg attendue (indicative) :")
print(
    "iceberg_demo/\n"
    "└── default/\n"
    "    └── customers/\n"
    "        ├── data/\n"
    "        │   └── 00000-*.parquet\n"
    "        └── metadata/\n"
    "            ├── v*.metadata.json\n"
    "            ├── snap-*.avro\n"
    "            └── manifest-*.avro\n"
)