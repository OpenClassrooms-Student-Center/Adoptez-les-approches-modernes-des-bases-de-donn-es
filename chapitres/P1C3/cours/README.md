# Gestion de fichiers avec AWS S3

Ce script Python permet de gérer des fichiers dans un bucket AWS S3. Il est organisé en **3 blocs indépendants** que vous pouvez exécuter séparément pour apprendre progressivement.

## 📋 Prérequis

Avant de commencer, assurez-vous d'avoir :

1. **AWS CLI configuré** avec vos credentials
   
   **Pour la plupart des étudiants** (credentials par défaut) :
   ```bash
   aws configure
   ```
   Vous devrez fournir :
   - AWS Access Key ID
   - AWS Secret Access Key
   - Région par défaut (ex: `eu-west-1`)
   
   **Optionnel : Utilisation d'un profil AWS** (pour les formateurs ou utilisateurs avancés) :
   
   Si vous utilisez plusieurs comptes AWS, vous pouvez créer des profils :
   ```bash
   aws configure --profile mon-profil
   ```
   
   Vous pourrez ensuite utiliser ce profil avec l'option `--profile` dans les commandes.

2. **Python 3 installé** et les packages nécessaires
   ```bash
   pip install boto3 pandas pyarrow
   ```
   - `boto3` : SDK AWS pour Python
   - `pandas` : Manipulation de données
   - `pyarrow` : Support du format Parquet

3. **Le fichier de données** `src/ventes.csv` présent dans le répertoire du script

## 🎯 Structure du code

Le script contient **4 fonctions principales**, correspondant aux blocs du cours :

| Bloc | Fonction | Description |
|------|----------|-------------|
| **Bloc 1** | `create_bucket()` | Crée un nouveau bucket S3 |
| **Bloc 2** | `upload_file()` | Upload un fichier local vers S3 |
| **Bloc 3** | `list_bucket()` | Liste les objets présents dans le bucket |
| **Bloc 4** | `process_pipeline()` | Traite un fichier brut : transforme et archive |

## 🚀 Utilisation en ligne de commande

### Méthode recommandée : exécuter bloc par bloc

Cette méthode vous permet de tester chaque étape individuellement et de comprendre le fonctionnement progressivement.

#### 📦 Bloc 1 : Créer un bucket S3

**Commande de base** (utilise le nom de bucket par défaut) :
```bash
python main.py create_bucket
```

**Avec un nom de bucket personnalisé** :
```bash
python main.py create_bucket --bucket mon-bucket-unique-12345
```

> 💡 **Note** : Le nom du bucket doit être **unique globalement** dans AWS. Si le nom existe déjà, vous obtiendrez une erreur. Choisissez un nom unique avec des chiffres ou des lettres.

**Ce qui se passe** :
- Un nouveau bucket S3 est créé dans votre région AWS configurée
- Un message de confirmation s'affiche si la création réussit

---

#### 📤 Bloc 2 : Uploader un fichier vers S3

**Commande de base** (utilise les paramètres par défaut) :
```bash
python main.py upload_file
```

Cette commande utilise par défaut :
- Bucket : `oc-datalake-8481716`
- Fichier local : `src/ventes.csv`
- Destination S3 : `raw/current/ventes.csv`

**Avec des paramètres personnalisés** :
```bash
python main.py upload_file \
  --bucket mon-bucket \
  --file src/ventes.csv \
  --s3-key raw/current/ventes.csv
```

**Avec un profil AWS** :
```bash
python main.py upload_file --profile mon-profil
```

**Explication des paramètres** :
- `--bucket` : Nom du bucket S3 (doit exister)
- `--file` : Chemin local du fichier à uploader
- `--s3-key` : Chemin de destination dans le bucket (structure de dossiers)

> ⚠️ **Important** : Le bucket doit exister avant d'y uploader des fichiers. Exécutez d'abord le Bloc 1 !

---

#### 📋 Bloc 3 : Lister les objets du bucket

**Commande de base** :
```bash
python main.py list_bucket
```

Cette commande liste tous les objets dans le dossier `raw/` du bucket par défaut.

**Avec des paramètres personnalisés** :
```bash
python main.py list_bucket \
  --bucket mon-bucket \
  --prefix raw/
```

**Avec un profil AWS** :
```bash
python main.py list_bucket --profile mon-profil
```

**Explication des paramètres** :
- `--bucket` : Nom du bucket S3 à lister
- `--prefix` : Filtre les objets commençant par ce préfixe (comme un dossier)

**Exemple de sortie** :
```
 - raw/current/ventes.csv
```

---

#### 🔄 Bloc 4 : Pipeline de traitement des données

Le pipeline de traitement illustre le principe d'un Data Lake avec deux zones :
- **raw/** : où arrivent les fichiers bruts (CSV)
- **processed/** : où sont stockés les fichiers transformés (Parquet)

**Commande de base** :
```bash
python main.py process_pipeline
```

Cette commande utilise par défaut :
- Fichier source : `raw/current/ventes.csv`
- Fichier transformé : `processed/ventes.parquet`

**Avec des paramètres personnalisés** :
```bash
python main.py process_pipeline \
  --bucket mon-bucket \
  --raw-key raw/current/ventes.csv \
  --processed-key processed/ventes.parquet
```

**Avec un profil AWS** :
```bash
python main.py process_pipeline --profile mon-profil
```

**Ce que fait le pipeline** :

Le pipeline effectue automatiquement les 6 étapes suivantes :

1. **📥 Téléchargement** : Télécharge le fichier CSV depuis `raw/current/`
2. **📊 Lecture et validation** : Lit le fichier avec pandas et affiche un aperçu
3. **🔧 Transformation** : Supprime les lignes avec valeurs manquantes (NaN)
4. **💾 Sauvegarde en Parquet** : Convertit et upload le fichier transformé dans `processed/`
5. **📦 Archivage** : Copie le fichier brut dans `raw/archived/` avec un timestamp
6. **🗑️ Nettoyage** : Supprime le fichier de `raw/current/` pour garder cette zone propre

**Exemple de sortie** :
```
🔄 Démarrage du pipeline de traitement...
   Fichier source: raw/current/ventes.csv

📥 Étape 1 : Téléchargement du fichier brut...
   ✅ Fichier téléchargé: ventes.csv

📊 Étape 2 : Lecture et validation du fichier...
   ✅ Fichier lu avec succès
   📈 Dimensions: 100 lignes, 5 colonnes

🔧 Étape 3 : Transformation des données...
   ✅ Transformation terminée
   🗑️  3 ligne(s) avec valeurs manquantes supprimée(s)
   📈 Dimensions finales: 97 lignes, 5 colonnes

💾 Étape 4 : Sauvegarde en format Parquet...
   ✅ Fichier transformé déposé dans: processed/ventes.parquet

📦 Étape 5 : Archivage du fichier brut...
   ✅ Fichier archivé dans: raw/archived/ventes_20241215_143022.csv

🗑️  Étape 6 : Suppression du fichier de raw/current/...
   ✅ Fichier supprimé de: raw/current/ventes.csv

✅ Pipeline terminé avec succès!
   📍 Fichier transformé: processed/ventes.parquet
   📍 Fichier archivé: raw/archived/ventes_20241215_143022.csv
```

**Explication des paramètres** :
- `--bucket` : Nom du bucket S3
- `--raw-key` : Clé S3 du fichier brut à traiter (dans `raw/current/`)
- `--processed-key` : Clé S3 de destination pour le fichier transformé (dans `processed/`)

**Organisation du Data Lake après traitement** :

```
bucket/
├── raw/
│   ├── current/          (vide après traitement)
│   └── archived/
│       └── ventes_20241215_143022.csv  (historique)
└── processed/
    └── ventes.parquet    (données transformées prêtes pour l'analyse)
```

> 💡 **Pourquoi ce pattern ?**
> - `raw/current/` reste propre avec uniquement les fichiers en attente
> - `raw/archived/` conserve un historique complet pour la traçabilité
> - `processed/` contient les données optimisées (Parquet) pour l'analyse
> - Format Parquet : plus efficace que CSV pour l'analyse (compression, colonnes)

---

#### 🔄 Exécuter tous les blocs en une fois

Si vous voulez exécuter les 3 blocs dans l'ordre :
```bash
python main.py all
```

**Avec un profil AWS** :
```bash
python main.py all --profile mon-profil
```

Cette commande exécute automatiquement :
1. Création du bucket
2. Upload du fichier
3. Liste des objets

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
from main import create_bucket, upload_file, list_bucket, process_pipeline
import os

# Bloc 1 : Création du bucket
bucket_name = "oc-datalake-8481716"
create_bucket(bucket_name)

# Bloc 2 : Upload du fichier
local_file = os.path.join("src", "ventes.csv")
upload_file(bucket_name, local_file, "raw/current/ventes.csv")

# Bloc 3 : Liste des objets
list_bucket(bucket_name, prefix="raw/")

# Bloc 4 : Pipeline de traitement
process_pipeline(bucket_name, "raw/current/ventes.csv")
```

---

## 📚 Détails des fonctions

### `create_bucket(bucket_name)`

Crée un nouveau bucket S3.

**Paramètres :**
- `bucket_name` (str) : Nom du bucket (doit être unique globalement dans AWS)

**Retour :** Aucun (affiche un message de confirmation)

**Exemple :**
```python
create_bucket("mon-bucket-unique-12345")
```

---

### `upload_file(bucket_name, local_file_path, s3_key)`

Upload un fichier local vers le bucket S3.

**Paramètres :**
- `bucket_name` (str) : Nom du bucket S3 (doit exister)
- `local_file_path` (str) : Chemin local du fichier à uploader
- `s3_key` (str) : Chemin de destination dans le bucket S3 (structure de dossiers)

**Retour :** Aucun (affiche un message de confirmation)

**Exemple :**
```python
upload_file("mon-bucket", "src/ventes.csv", "raw/current/ventes.csv")
```

---

### `list_bucket(bucket_name, prefix="")`

Liste les objets dans un bucket S3.

**Paramètres :**
- `bucket_name` (str) : Nom du bucket S3
- `prefix` (str, optionnel) : Préfixe pour filtrer les objets (par défaut : chaîne vide)

**Retour :** Aucun (affiche la liste des objets)

**Exemple :**
```python
list_bucket("mon-bucket", prefix="raw/")
```

---

### `process_pipeline(bucket_name, raw_key, processed_key=None)`

Traite un fichier brut du Data Lake : télécharge, transforme, sauvegarde en Parquet et archive.

**Paramètres :**
- `bucket_name` (str) : Nom du bucket S3
- `raw_key` (str) : Clé S3 du fichier brut dans `raw/current/` (ex: `"raw/current/ventes.csv"`)
- `processed_key` (str, optionnel) : Clé S3 de destination dans `processed/` (par défaut: `"processed/ventes.parquet"`)

**Retour :** Aucun (affiche les étapes du pipeline)

**Exemple :**
```python
process_pipeline("mon-bucket", "raw/current/ventes.csv")
```

**Ce que fait la fonction :**
1. Télécharge le fichier CSV depuis `raw/current/`
2. Lit et valide le contenu avec pandas
3. Transforme les données (suppression des NaN)
4. Sauvegarde en Parquet dans `processed/`
5. Archive le fichier brut dans `raw/archived/` avec timestamp
6. Supprime le fichier de `raw/current/`

---

## ⚠️ Notes importantes et dépannage

### Erreurs courantes

1. **"BucketAlreadyExists"** ou **"BucketAlreadyOwnedByYou"**
   - Le nom du bucket existe déjà. Choisissez un nom unique.
   - Solution : Utilisez `--bucket` avec un nom différent

2. **"NoSuchBucket"**
   - Vous essayez d'uploader ou lister un bucket qui n'existe pas.
   - Solution : Créez d'abord le bucket avec `create_bucket`

3. **"FileNotFoundError"**
   - Le fichier local spécifié n'existe pas.
   - Solution : Vérifiez le chemin du fichier avec `--file`

4. **"AccessDenied"** ou **"InvalidAccessKeyId"**
   - Problème de credentials AWS.
   - Solution : Vérifiez votre configuration avec `aws configure`
   - Si vous utilisez un profil : vérifiez que le profil existe avec `aws configure list-profiles`

5. **"ProfileNotFound"**
   - Le profil AWS spécifié n'existe pas.
   - Solution : Vérifiez le nom du profil ou créez-le avec `aws configure --profile nom-profil`

6. **"NoSuchKey"** (lors du pipeline)
   - Le fichier brut n'existe pas dans `raw/current/`.
   - Solution : Assurez-vous d'avoir uploadé le fichier avec `upload_file` avant de lancer le pipeline

7. **"ModuleNotFoundError: No module named 'pandas'"** ou **"No module named 'pyarrow'"**
   - Les dépendances pandas ou pyarrow ne sont pas installées.
   - Solution : Installez-les avec `pip install pandas pyarrow`

### Utilisation des profils AWS (optionnel)

Si vous travaillez avec plusieurs comptes AWS ou environnements, vous pouvez utiliser des profils :

**Créer un profil** :
```bash
aws configure --profile mon-profil
```

**Lister vos profils** :
```bash
aws configure list-profiles
```

**Utiliser un profil dans le script** :
```bash
python main.py create_bucket --profile mon-profil
```

> 💡 **Note pour les étudiants** : Si vous n'utilisez qu'un seul compte AWS, vous n'avez pas besoin d'utiliser `--profile`. Le script utilisera automatiquement vos credentials par défaut configurés avec `aws configure`.

### Bonnes pratiques

- ✅ Le nom du bucket doit être **unique globalement** dans AWS
- ✅ Utilisez des noms de bucket en minuscules, sans espaces
- ✅ Assurez-vous d'avoir les **permissions nécessaires** sur votre compte AWS
- ✅ Le bucket doit être **créé avant** d'y uploader des fichiers
- ✅ Utilisez des préfixes (comme `raw/current/`) pour organiser vos fichiers
- ✅ Utilisez `--profile` si vous travaillez avec plusieurs comptes AWS

### Ordre d'exécution recommandé

Pour suivre le cours, exécutez les blocs dans cet ordre :

1. **Bloc 1** : `python main.py create_bucket`
2. **Bloc 2** : `python main.py upload_file`
3. **Bloc 3** : `python main.py list_bucket`
4. **Bloc 4** : `python main.py process_pipeline` (traite le fichier uploadé dans le Bloc 2)

---

## 🔍 Vérification

Après avoir exécuté les blocs, vous pouvez vérifier dans la console AWS :
1. Connectez-vous à [AWS Console](https://console.aws.amazon.com/)
2. Allez dans le service **S3**
3. Ouvrez votre bucket

**Après les Blocs 1-3** :
- Vous devriez voir le fichier `raw/current/ventes.csv`

**Après le Bloc 4 (pipeline)** :
- `raw/current/` devrait être vide
- `raw/archived/` devrait contenir le fichier archivé avec timestamp
- `processed/` devrait contenir le fichier `ventes.parquet`
