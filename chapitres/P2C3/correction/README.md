# Exercice – Manipuler une table Delta Lake et son historique

## 🎯 Objectif pédagogique
Cet exercice a pour but de vous faire manipuler **Delta Lake en Python** afin de :
- créer une table Delta locale à partir d’un fichier CSV,
- modifier les données (append, update, delete, merge),
- comprendre le **versioning** de Delta Lake,
- utiliser le **time travel** pour relire une version passée.

À la fin de l’exercice, vous saurez expliquer comment Delta Lake garantit la traçabilité et la reproductibilité des données.

---

## 📁 Contenu du dossier

```text
.
├── sensor.csv                 # Données initiales (avec une ligne obsolète)
├── corrige_p2c3_delta_sensors.py   # Script de correction
├── data/
│   └── sensors_delta/         # Table Delta Lake (créée à l’exécution)
└── README.md                  # Ce fichier
```

---

## 📄 Fichier de données (`sensor.csv`)

Le fichier CSV contient volontairement une **ligne obsolète** (parcelle `Old-9`) qui sera supprimée pendant l’exercice.

```csv
sensor_id,humidity,parcel
101,41.3,North-1
102,46.8,East-2
103,44.5,South-3
999,50.0,Old-9
```

---

## ⚙️ Prérequis

- Python **3.12** recommandé
- Environnement virtuel activé
- Librairies :
```bash
pip install pandas pyarrow deltalake
```

---

## ▶️ Exécution

Lancez simplement le script de correction :

```bash
python corrige_delta_sensors.py
```

Le script est **rejouable** : la table Delta est supprimée et recréée à chaque exécution.

---

## 🧪 Étapes réalisées dans le script

1. **Création de la table Delta** à partir du CSV  
2. **Ajout (append)** de nouveaux capteurs (104 et 105)  
3. **Correction (update)** d’une mesure erronée (`sensor_id = 101`)  
4. **Suppression (delete)** de la ligne obsolète (`parcel = 'Old-9'`)  
5. **Fusion (merge)** :
   - mise à jour de `sensor_id = 102`
   - insertion d’un nouveau capteur `sensor_id = 106`  
6. **Exploration de l’historique** (`table.history()`)  
7. **Time travel** : lecture de la version 0 pour comparaison

---

## 📊 Résultat final attendu

| sensor_id | humidity | parcel   |
|----------:|---------:|----------|
| 101 | 145.2 | North-1 |
| 102 | 47.0  | East-2  |
| 103 | 44.5  | South-3 |
| 104 | 49.2  | West-1  |
| 105 | 43.0  | North-2 |
| 106 | 41.9  | West-3  |

---

## 🕒 Versions et historique

- Les **opérations d’écriture** (append, update, delete, merge) créent des **versions Delta**.
- L’historique affiche **5 versions d’écriture** (0 à 4).
- La lecture de la version 0 via le **time travel** constitue une **6ᵉ étape**, mais **ne crée pas de nouvelle version**.

> 💡 À retenir : Delta Lake permet de revenir à n’importe quelle version passée sans dupliquer les données.

---

## ✅ À retenir
- Delta Lake combine la simplicité du data lake avec les garanties d’une base transactionnelle.
- Chaque modification est tracée et versionnée.
- Le time travel est un outil clé pour l’audit, le debug et la reproductibilité des analyses.

Bon apprentissage 🚀
