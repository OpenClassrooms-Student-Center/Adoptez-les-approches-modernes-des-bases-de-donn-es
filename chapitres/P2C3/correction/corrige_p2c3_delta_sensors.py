from __future__ import annotations

from pathlib import Path
import shutil

import pandas as pd
from datetime import datetime, timezone
from deltalake import DeltaTable, write_deltalake


def print_title(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def pretty_history(table: DeltaTable) -> pd.DataFrame:
    """Transforme table.history() en DataFrame lisible (colonnes utiles)."""
    history = table.history()
    rows = []
    for h in history:
        rows.append({
            "version": h.get("version"),
            "operation": h.get("operation"),
            "timestamp_utc": datetime.fromtimestamp(
                h.get("timestamp", 0) / 1000, tz=timezone.utc
            ).isoformat(),
            "readVersion": h.get("readVersion"),
            "mode": (h.get("operationParameters") or {}).get("mode"),
            "predicate": (h.get("operationParameters") or {}).get("predicate"),
            "mergePredicate": (h.get("operationParameters") or {}).get("mergePredicate"),
            "num_added_rows": (h.get("operationMetrics") or {}).get("num_added_rows"),
            "num_output_rows": (h.get("operationMetrics") or {}).get("num_output_rows"),
            "num_updated_rows": (h.get("operationMetrics") or {}).get("num_updated_rows"),
            "num_deleted_rows": (h.get("operationMetrics") or {}).get("num_deleted_rows"),
            "execution_time_ms": (h.get("operationMetrics") or {}).get("execution_time_ms"),
            "engineInfo": h.get("engineInfo"),
        })
    return pd.DataFrame(rows).sort_values("version")


# -----------------------------
# Paths
# -----------------------------
BASE = Path(__file__).parent
CSV_PATH = BASE / "sensors.csv"
DELTA_PATH = BASE / "data" / "sensors_delta"
DELTA_PATH.parent.mkdir(parents=True, exist_ok=True)

# Rejouable : on repart de zéro
if DELTA_PATH.exists():
    shutil.rmtree(DELTA_PATH)


# -----------------------------
# 1) Créer la table Delta
# -----------------------------
print_title("1) Créer la table Delta depuis sensors.csv")

df = pd.read_csv(CSV_PATH)
df = df[["sensor_id", "humidity", "parcel"]]  # ordre canonique

write_deltalake(str(DELTA_PATH), df, mode="overwrite")
table = DeltaTable(str(DELTA_PATH))

print("✅ Table Delta créée (version 0)")
print(table.to_pandas().sort_values("sensor_id").reset_index(drop=True))
print("Version actuelle:", table.version())


# -----------------------------
# 2) Ajouter de nouvelles mesures (append)
# -----------------------------
print_title("2) Append : simuler l’arrivée des capteurs 104 et 105")

new_data = pd.DataFrame({
    "sensor_id": [104, 105],
    "humidity": [49.2, 43.0],
    "parcel": ["West-1", "North-2"],
})[["sensor_id", "humidity", "parcel"]]

write_deltalake(str(DELTA_PATH), new_data, mode="append")
table = DeltaTable(str(DELTA_PATH))

print("✅ Nouvelles mesures ajoutées (append) → nouvelle version")
print("Version actuelle:", table.version())


# -----------------------------
# 3) Corriger une erreur (update)
# -----------------------------
print_title("3) Update : corriger sensor_id=101 (humidity -> 145.2)")

table.update(
    predicate="sensor_id = 101",
    updates={"humidity": "145.2"}
)
table = DeltaTable(str(DELTA_PATH))

print("✅ Mesure corrigée → nouvelle version")
print("Version actuelle:", table.version())


# -----------------------------
# 4) Supprimer une ligne obsolète (delete)
# -----------------------------
print_title("4) Delete : supprimer la parcelle obsolète 'Old-9'")

table.delete("parcel = 'Old-9'")
table = DeltaTable(str(DELTA_PATH))

print("✅ Parcelle obsolète supprimée → nouvelle version")
print("Version actuelle:", table.version())


# -----------------------------
# 5) Fusionner de nouvelles données (merge)
# -----------------------------
print_title("5) Merge : corriger sensor_id=102 et ajouter sensor_id=106")

updates_df = pd.DataFrame({
    "sensor_id": [102, 106],
    "humidity": [47.0, 41.9],
    "parcel": ["East-2", "West-3"],
})[["sensor_id", "humidity", "parcel"]]

(
    table.merge(
        source=updates_df,
        predicate="source.sensor_id = target.sensor_id",
        source_alias="source",
        target_alias="target",
    )
    .when_matched_update(
        updates={
            "humidity": "source.humidity",
            "parcel": "source.parcel",
        }
    )
    .when_not_matched_insert_all()
    .execute()
)

table = DeltaTable(str(DELTA_PATH))

print("✅ Merge terminé → nouvelle version")
print("Version actuelle:", table.version())


# -----------------------------
# 6) Historique + time travel (lecture version 0)
# -----------------------------
print_title("6) Historique (versions d’écriture) + time travel (lecture version 0)")

hist_df = pretty_history(table)
print(hist_df.to_string(index=False))

print_title("Lecture de la version 0 (table d'origine)")
table_v0 = DeltaTable(str(DELTA_PATH), version=0)
print(table_v0.to_pandas().sort_values("sensor_id").reset_index(drop=True))

print_title("Table finale (dernière version)")
final_df = table.to_pandas().sort_values("sensor_id").reset_index(drop=True)
print(final_df)


# -----------------------------
# Contrôle : résultat attendu
# -----------------------------
print_title("Résultat attendu (pour comparaison)")

expected = pd.DataFrame({
    "sensor_id": [101, 102, 103, 104, 105, 106],
    "humidity": [145.2, 47.0, 44.5, 49.2, 43.0, 41.9],
    "parcel": ["North-1", "East-2", "South-3", "West-1", "North-2", "West-3"],
})

print(expected)

# Vérification simple (tolérance sur float)
ok_ids = final_df["sensor_id"].tolist() == expected["sensor_id"].tolist()
ok_parcels = final_df["parcel"].tolist() == expected["parcel"].tolist()
ok_humidity = all(abs(final_df["humidity"] - expected["humidity"]) < 1e-9)

print("\n✅ Résultat final conforme ?", bool(ok_ids and ok_parcels and ok_humidity))

# Vérification du nombre de versions d’écriture
print("\n✅ Versions disponibles dans l'historique :", hist_df['version'].tolist())
