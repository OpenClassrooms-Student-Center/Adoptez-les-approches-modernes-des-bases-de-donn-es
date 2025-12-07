# Pipeline de traitement IoT - GreenFarm Data Lake (MinIO)

Ce script Python permet de traiter des données IoT issues de capteurs installés dans les champs de GreenFarm. Il gère un pipeline complet de traitement de données JSON vers Parquet dans un Data Lake MinIO (compatible S3).

## 📋 Prérequis

Avant de commencer, assurez-vous d'avoir :

1. **MinIO installé et démarré**
   - MinIO doit être accessible (par défaut : `http://localhost:9000`)
   - Vous devez connaître vos credentials MinIO (Access Key et Secret Key)
   - Par défaut, MinIO utilise `minioadmin` / `minioadmin`

2. **Python 3 installé** et les packages nécessaires
   ```bash
   pip install boto3 pandas pyarrow
   ```
   - `boto3` : SDK compatible S3 pour Python (fonctionne avec MinIO)
   - `pandas` : Manipulation de données
   - `pyarrow` : Support du format Parquet

3. **Le fichier de données** `src/iot.json` présent dans le répertoire du script

## 🎯 Structure du code

Le script contient **4 fonctions principales** :

| Fonction | Description |
|----------|-------------|
| `create_bucket()` | Crée un nouveau bucket MinIO |
| `upload_file()` | Upload un fichier local vers MinIO |
| `process_pipeline()` | Traite un fichier JSON IoT : transforme et archive |
| `list_bucket()` | Liste les objets présents dans le bucket |

## 🚀 Utilisation en ligne de commande

### Méthode recommandée : exécuter bloc par bloc

#### 📦 Étape 1 : Créer un bucket MinIO

**⚠️ Important** : Remplacez `datalex` par vos initiales ou un identifiant personnel.

**Commande de base** :
```bash
python main.py create_bucket
```

Cette commande utilise par défaut :
- Endpoint MinIO : `http://localhost:9000`
- Access Key : `minioadmin`
- Secret Key : `minioadmin`

**Avec un nom de bucket personnalisé** :
```bash
python main.py create_bucket --bucket greenfarm-datalake-demo-vos-initiales
```

**Avec un endpoint MinIO personnalisé** :
```bash
python main.py create_bucket \
  --endpoint http://minio.example.com:9000 \
  --access-key votre-access-key \
  --secret-key votre-secret-key
```

**Ce qui se passe** :
- Un nouveau bucket est créé dans votre instance MinIO
- Un message de confirmation s'affiche si la création réussit

---

#### 📤 Étape 2 : Uploader le fichier JSON vers MinIO

**Commande de base** :
```bash
python main.py upload_file
```

Cette commande utilise par défaut :
- Bucket : `greenfarm-datalake-demo-datalex`
- Fichier local : `src/iot.json`
- Destination : `raw/current/iot.json`
- Endpoint : `http://localhost:9000`

**Avec des paramètres personnalisés** :
```bash
python main.py upload_file \
  --bucket greenfarm-datalake-demo-vos-initiales \
  --file src/iot.json \
  --s3-key raw/current/iot.json \
  --endpoint http://minio.example.com:9000
```

> ⚠️ **Important** : Le bucket doit exister avant d'y uploader des fichiers. Exécutez d'abord l'étape 1 !

---

#### 🔄 Étape 3 : Pipeline de traitement des données IoT

Le pipeline effectue les transformations suivantes sur les données IoT :
- Conversion des timestamps au bon format
- Renommage des colonnes (temperature → temp_c)
- Tri par device_id et timestamp
- Calcul d'une moyenne glissante sur 3 mesures (temp_c_roll3)

**Commande de base** :
```bash
python main.py process_pipeline
```

Cette commande utilise par défaut :
- Fichier source : `raw/current/iot.json`
- Fichier transformé : `processed/iot.parquet`

**Avec des paramètres personnalisés** :
```bash
python main.py process_pipeline \
  --bucket greenfarm-datalake-demo-vos-initiales \
  --raw-key raw/current/iot.json \
  --processed-key processed/iot.parquet \
  --endpoint http://minio.example.com:9000
```

**Ce que fait le pipeline** :

Le pipeline effectue automatiquement les 6 étapes suivantes :

1. **📥 Téléchargement** : Télécharge le fichier JSON depuis `raw/current/`
2. **📊 Lecture et validation** : Lit le fichier avec pandas et affiche un aperçu
3. **🔧 Transformation** :
   - Conversion des timestamps
   - Renommage des colonnes
   - Tri des données
   - Calcul de la moyenne glissante
4. **💾 Sauvegarde en Parquet** : Convertit et upload le fichier transformé dans `processed/`
5. **📦 Archivage** : Copie le fichier brut dans `raw/archived/` avec un timestamp
6. **🗑️ Nettoyage** : Supprime le fichier de `raw/current/` pour garder cette zone propre

**Exemple de sortie** :
```
🔄 Démarrage du pipeline de traitement IoT...
   Fichier source: raw/current/iot.json

Étape 1 : Téléchargement du fichier brut...
   ✅ Fichier téléchargé: iot.json

Étape 2 : Lecture et validation du fichier...
   ✅ Fichier lu avec succès
   Dimensions: 100 lignes, 4 colonnes

   Aperçu des données:
   ...

Étape 3 : Transformation des données...
   - Conversion des timestamps...
   - Renommage des colonnes...
   - Tri des données par device_id et timestamp...
   - Calcul de la moyenne glissante (rolling 3)...
   ✅ Transformation terminée
   Dimensions finales: 100 lignes, 5 colonnes

Étape 4 : Sauvegarde en format Parquet...
   ✅ Fichier transformé déposé dans: processed/iot.parquet

Étape 5 : Archivage du fichier brut...
   ✅ Fichier archivé dans: raw/archived/iot_20241215_143022.json

Étape 6 : Suppression du fichier de raw/current/...
   ✅ Fichier supprimé de: raw/current/iot.json

✅ Pipeline terminé avec succès!
   📍 Fichier transformé: processed/iot.parquet
   📍 Fichier archivé: raw/archived/iot_20241215_143022.json
```

---

#### 📋 Étape 4 : Lister les objets du bucket

**Commande de base** :
```bash
python main.py list_bucket
```

Cette commande liste tous les objets dans le dossier `raw/` du bucket par défaut.

**Avec des paramètres personnalisés** :
```bash
python main.py list_bucket \
  --bucket greenfarm-datalake-demo-vos-initiales \
  --prefix processed/
```

---

#### 🔄 Exécuter toutes les étapes en une fois

Si vous voulez exécuter toutes les étapes dans l'ordre :
```bash
python main.py all
```

**Avec un endpoint MinIO personnalisé** :
```bash
python main.py all \
  --endpoint http://minio.example.com:9000 \
  --access-key votre-access-key \
  --secret-key votre-secret-key
```

Cette commande exécute automatiquement :
1. Création du bucket
2. Upload du fichier
3. Pipeline de traitement
4. Liste des objets

---

### 📖 Obtenir de l'aide

Pour voir toutes les options disponibles :
```bash
python main.py --help
```

Pour voir l'aide d'une action spécifique :
```bash
python main.py create_bucket --help
```

---

## 💻 Utilisation dans un script Python

Vous pouvez également importer les fonctions dans vos propres scripts Python ou notebooks Jupyter :

```python
from main import create_bucket, upload_file, process_iot_pipeline, list_bucket
import os

# Étape 1 : Création du bucket
bucket_name = "greenfarm-datalake-demo-vos-initiales"
create_bucket(bucket_name)

# Étape 2 : Upload du fichier
local_file = os.path.join("src", "iot.json")
upload_file(bucket_name, local_file, "raw/current/iot.json")

# Étape 3 : Pipeline de traitement
process_iot_pipeline(bucket_name, "raw/current/iot.json")

# Étape 4 : Liste des objets
list_bucket(bucket_name, prefix="processed/")
```

---

## 📚 Détails des transformations

Le pipeline applique les transformations suivantes aux données IoT :

1. **Conversion des timestamps** : `pd.to_datetime()` pour convertir les timestamps en format datetime
2. **Renommage** : `temperature` → `temp_c` pour plus de clarté
3. **Tri** : Par `device_id` puis par `timestamp` pour un ordre logique
4. **Moyenne glissante** : Calcul de `temp_c_roll3` (moyenne sur 3 mesures) par device

Ces transformations préparent les données pour l'analyse et l'exploitation.

---

## ⚠️ Notes importantes et dépannage

### Erreurs courantes

1. **"BucketAlreadyExists"** ou **"BucketAlreadyOwnedByYou"**
   - Le nom du bucket existe déjà. Choisissez un nom unique.
   - Solution : Utilisez `--bucket` avec un nom différent (remplacez 'datalex' par vos initiales)

2. **"NoSuchBucket"**
   - Vous essayez d'uploader ou traiter un bucket qui n'existe pas.
   - Solution : Créez d'abord le bucket avec `create_bucket`

3. **"FileNotFoundError"**
   - Le fichier local spécifié n'existe pas.
   - Solution : Vérifiez le chemin du fichier avec `--file`

4. **"AccessDenied"** ou **"InvalidAccessKeyId"**
   - Problème de credentials MinIO.
   - Solution : Vérifiez vos credentials avec `--access-key` et `--secret-key`
   - Par défaut, MinIO utilise `minioadmin` / `minioadmin`

5. **"Connection refused"** ou **"Could not connect"**
   - MinIO n'est pas accessible à l'endpoint spécifié.
   - Solution : Vérifiez que MinIO est démarré et accessible
   - Vérifiez l'endpoint avec `--endpoint` (défaut: `http://localhost:9000`)

6. **"NoSuchKey"** (lors du pipeline)
   - Le fichier brut n'existe pas dans `raw/current/`.
   - Solution : Assurez-vous d'avoir uploadé le fichier avec `upload_file` avant de lancer le pipeline

7. **"ModuleNotFoundError: No module named 'pandas'"** ou **"No module named 'pyarrow'"**
   - Les dépendances pandas ou pyarrow ne sont pas installées.
   - Solution : Installez-les avec `pip install pandas pyarrow`

### Bonnes pratiques

- ✅ Utilisez des noms de bucket en minuscules, sans espaces
- ✅ Remplacez `datalex` dans le nom du bucket par vos initiales ou un identifiant personnel
- ✅ Assurez-vous que MinIO est démarré et accessible avant d'exécuter les commandes
- ✅ Le bucket doit être **créé avant** d'y uploader des fichiers
- ✅ Utilisez des préfixes (comme `raw/current/`) pour organiser vos fichiers
- ✅ Utilisez `--endpoint`, `--access-key` et `--secret-key` pour vous connecter à différentes instances MinIO
- ✅ Pour la production, utilisez `--use-ssl` pour activer SSL/TLS

### Ordre d'exécution recommandé

Pour suivre l'exercice, exécutez les étapes dans cet ordre :

1. **Étape 1** : `python main.py create_bucket --bucket greenfarm-datalake-demo-vos-initiales`
2. **Étape 2** : `python main.py upload_file`
3. **Étape 3** : `python main.py process_pipeline`
4. **Étape 4** : `python main.py list_bucket`

---

## 🔍 Vérification

Après avoir exécuté toutes les étapes, vous pouvez vérifier dans la console MinIO :
1. Accédez à votre console MinIO (par défaut : `http://localhost:9001`)
2. Connectez-vous avec vos credentials MinIO
3. Ouvrez votre bucket

**Structure attendue** :
- `raw/current/` devrait être vide
- `raw/archived/` devrait contenir le fichier archivé avec timestamp
- `processed/` devrait contenir le fichier `iot.parquet`

**Ou via la ligne de commande** :
```bash
python main.py list_bucket --prefix raw/
python main.py list_bucket --prefix processed/
```

---

## 📝 Contexte de l'exercice

GreenFarm continue sa croissance et souhaite mieux exploiter ses données pour optimiser ses cultures et sa distribution. Ce pipeline traite des données issues de capteurs IoT installés dans les champs, qui collectent régulièrement des informations comme la température, l'humidité du sol ou le taux d'ensoleillement.

Les données sont exportées chaque jour sous forme de fichier JSON et traitées par ce pipeline pour être transformées en format Parquet optimisé pour l'analyse.

