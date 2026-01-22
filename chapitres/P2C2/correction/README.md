# Exercice — Comparer Delta Lake et Apache Iceberg en local

Ce mini-projet a pour objectif de **manipuler deux open table formats modernes** — **Delta Lake** et **Apache Iceberg** — à partir d’un même jeu de données CSV, afin de comparer leur fonctionnement (versions, snapshots, stockage sur disque).

---

## 🎯 Objectifs pédagogiques

À la fin de cet exercice, vous serez capable de :

- créer une **table Delta Lake locale** à partir d’un fichier CSV ;
- ajouter des données et utiliser le **time travel** pour relire une version précédente ;
- créer une **table Iceberg locale** avec un catalogue SQL (SQLite) ;
- écrire des données et **inspecter les snapshots Iceberg** ;
- comprendre les différences de gestion des versions entre Delta Lake et Iceberg.

---

## 📁 Contenu du dossier

```text
p2c2/
├── clients.csv
├── corrige_p2c2_delta_iceberg.py
├── README.md
├── delta_clients/          # table Delta Lake (créée à l'exécution)
├── iceberg_demo/           # warehouse Iceberg (créé à l'exécution)
└── iceberg_catalog.db      # catalogue Iceberg (SQLite)
```

---

## ⚙️ Pré-requis

- Python **3.12** recommandé
- Un environnement virtuel activé

### Installation des dépendances

```bash
pip install pandas pyarrow deltalake pyiceberg "pyiceberg[sql-sqlite]"
```

---

## ▶️ Exécution de l’exercice

Assurez-vous que le fichier **`clients.csv`** soit bien présent dans le dossier, puis lancez :

```bash
python corrige_p2c2_delta_iceberg.py
```

Le script est **idempotent** : il peut être relancé plusieurs fois sans casser l’exécution.

---

## 🔍 Ce que fait le script

### 1. Delta Lake
- Crée une table Delta Lake locale à partir de `clients.csv`
- Ajoute deux nouveaux clients
- Affiche le **numéro de version**
- Relit la **version 0** grâce au *time travel*

📁 Structure observée :
```text
delta_clients/
├── _delta_log/
└── part-*.parquet
```

---

### 2. Apache Iceberg
- Crée une table Iceberg équivalente en local
- Utilise un **catalogue SQL (SQLite)** pour les métadonnées
- Ajoute les données via **PyArrow**
- Liste les **snapshots** disponibles

📁 Structure observée :
```text
iceberg_demo/
└── default/
    └── customers/
        ├── data/
        │   └── 00000-*.parquet
        └── metadata/
            ├── v*.metadata.json
            ├── snap-*.avro
            └── manifest-*.avro
```

---

## 🧠 À retenir

- **Delta Lake** utilise un journal transactionnel (`_delta_log`) et expose des versions numérotées.
- **Iceberg** repose sur des snapshots complets décrits par des métadonnées hiérarchisées.
- Les deux formats apportent des garanties ACID au data lake, mais avec des **architectures internes différentes**.
- Iceberg nécessite toujours un **catalogue**, même en local.

---

## 💡 Pour aller plus loin

- Comparer le contenu des métadonnées (`_delta_log` vs `metadata/`)
- Ajouter une seconde écriture Iceberg et observer les nouveaux snapshots