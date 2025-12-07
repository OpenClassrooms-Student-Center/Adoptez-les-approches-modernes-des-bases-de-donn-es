import boto3
import os
import argparse
import datetime
from botocore.exceptions import ClientError
import pandas as pd


def create_bucket(bucket_name):
    """
    Crée un bucket MinIO avec le nom spécifié.
    
    Args:
        bucket_name (str): Nom du bucket
    
    Returns:
        None
    
    Raises:
        ClientError: En cas d'erreur (bucket existe déjà, conflit, etc.)
    """
    global s3
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket {bucket_name} créé avec succès.")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'BucketAlreadyExists':
            print(f"⚠️  Le bucket {bucket_name} existe déjà.")
            print("   Vous pouvez continuer avec les autres opérations.")
        elif error_code == 'BucketAlreadyOwnedByYou':
            print(f"✅ Le bucket {bucket_name} vous appartient déjà.")
            print("   Vous pouvez continuer avec les autres opérations.")
        elif error_code == 'OperationAborted':
            print(f"⚠️  Une opération est en cours sur le bucket {bucket_name}.")
            print("   Veuillez attendre quelques instants et réessayer.")
        else:
            print(f"❌ Erreur lors de la création du bucket {bucket_name}:")
            print(f"   {e.response.get('Error', {}).get('Message', str(e))}")
            raise


def upload_file(bucket_name, local_file_path, s3_key):
    """
    Upload un fichier vers un bucket MinIO.
    
    Args:
        bucket_name (str): Nom du bucket
        local_file_path (str): Chemin local du fichier à uploader
        s3_key (str): Clé (chemin) dans le bucket
    
    Returns:
        None
    
    Raises:
        FileNotFoundError: Si le fichier local n'existe pas
        ClientError: En cas d'erreur
    """
    global s3
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Le fichier {local_file_path} n'existe pas.")
    
    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"✅ Fichier {local_file_path} envoyé dans {s3_key}.")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchBucket':
            print(f"❌ Le bucket {bucket_name} n'existe pas.")
            print("   Créez d'abord le bucket avec: python main.py create_bucket")
        else:
            print(f"❌ Erreur lors de l'upload du fichier:")
            print(f"   {e.response.get('Error', {}).get('Message', str(e))}")
        raise


def process_iot_pipeline(bucket_name, raw_key, processed_key=None):
    """
    Traite un fichier JSON IoT du Data Lake : télécharge, transforme, sauvegarde en Parquet et archive.
    
    Le pipeline effectue les étapes suivantes :
    1. Télécharge le fichier JSON depuis raw/current/
    2. Lit et valide le contenu avec pandas
    3. Transforme les données :
       - Convertit les timestamps
       - Renomme les colonnes
       - Trie les données
       - Calcule une moyenne glissante
    4. Sauvegarde en Parquet dans processed/
    5. Archive le fichier brut dans raw/archived/ avec timestamp
    6. Supprime le fichier de raw/current/
    
    Args:
        bucket_name (str): Nom du bucket
        raw_key (str): Clé du fichier brut dans raw/current/ (ex: "raw/current/iot.json")
        processed_key (str, optionnel): Clé de destination dans processed/ 
                                       (par défaut: "processed/iot.parquet")
    
    Returns:
        None
    
    Raises:
        ClientError: En cas d'erreur
        FileNotFoundError: Si le fichier brut n'existe pas
        ImportError: Si pyarrow n'est pas installé
    """
    global s3
    
    # Vérifier que pyarrow est installé
    try:
        import pyarrow
    except ImportError:
        print("❌ Erreur : pyarrow n'est pas installé.")
        print("   Le format Parquet nécessite pyarrow.")
        print("   Installez-le avec : pip install pyarrow")
        raise ImportError(
            "pyarrow est requis pour le support Parquet. "
            "Installez-le avec: pip install pyarrow"
        )
    
    # Déterminer le nom du fichier et la clé de destination
    filename = os.path.basename(raw_key)
    base_name = os.path.splitext(filename)[0]  # "iot" sans extension
    
    if processed_key is None:
        processed_key = f"processed/{base_name}.parquet"
    
    local_file = filename  # "iot.json"
    local_parquet = f"{base_name}.parquet"  # "iot.parquet"
    
    try:
        print(f"🔄 Démarrage du pipeline de traitement IoT...")
        print(f"   Fichier source: {raw_key}")
        
        # Étape 1 : Télécharger le fichier brut
        print(f"\nÉtape 1 : Téléchargement du fichier brut...")
        s3.download_file(bucket_name, raw_key, local_file)
        print(f"   ✅ Fichier téléchargé: {local_file}")
        
        # Étape 2 : Lire et valider le fichier
        print(f"\nÉtape 2 : Lecture et validation du fichier...")
        df = pd.read_json(local_file)
        print(f"   ✅ Fichier lu avec succès")
        print(f"   Dimensions: {df.shape[0]} lignes, {df.shape[1]} colonnes")
        print(f"\n   Aperçu des données:")
        print(df.head().to_string())
        
        # Étape 3 : Transformer les données
        print(f"\nÉtape 3 : Transformation des données...")
        
        # Convertir timestamps
        print("   - Conversion des timestamps...")
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # Renommer la colonne temperature
        print("   - Renommage des colonnes...")
        df = df.rename(columns={"temperature": "temp_c"})
        
        # Ordonnancement logique
        print("   - Tri des données par device_id et timestamp...")
        df = df.sort_values(["device_id", "timestamp"])
        
        # Calcul moyenne glissante sur 3 mesures
        print("   - Calcul de la moyenne glissante (rolling 3)...")
        df["temp_c_roll3"] = (
            df.groupby("device_id")["temp_c"]
            .rolling(3, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
        
        print(f"   ✅ Transformation terminée")
        print(f"   Dimensions finales: {df.shape[0]} lignes, {df.shape[1]} colonnes")
        print(f"\n   Aperçu des données transformées:")
        print(df.head().to_string())
        
        # Étape 4 : Sauvegarder en Parquet
        print(f"\nÉtape 4 : Sauvegarde en format Parquet...")
        df.to_parquet(local_parquet)
        s3.upload_file(local_parquet, bucket_name, processed_key)
        print(f"   ✅ Fichier transformé déposé dans: {processed_key}")
        
        # Étape 5 : Archiver le fichier brut
        print(f"\nÉtape 5 : Archivage du fichier brut...")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_key = f"raw/archived/{base_name}_{timestamp}.json"
        
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": raw_key},
            Key=archived_key
        )
        print(f"   ✅ Fichier archivé dans: {archived_key}")
        
        # Étape 6 : Supprimer le fichier de raw/current/
        print(f"\nÉtape 6 : Suppression du fichier de raw/current/...")
        s3.delete_object(Bucket=bucket_name, Key=raw_key)
        print(f"   ✅ Fichier supprimé de: {raw_key}")
        
        # Nettoyage des fichiers locaux
        if os.path.exists(local_file):
            os.remove(local_file)
        if os.path.exists(local_parquet):
            os.remove(local_parquet)
        
        print(f"\n✅ Pipeline terminé avec succès!")
        print(f"   📍 Fichier transformé: {processed_key}")
        print(f"   📍 Fichier archivé: {archived_key}")
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchKey':
            print(f"❌ Le fichier {raw_key} n'existe pas dans le bucket.")
            print("   Assurez-vous que le fichier a été uploadé dans raw/current/")
        elif error_code == 'NoSuchBucket':
            print(f"❌ Le bucket {bucket_name} n'existe pas.")
            print("   Créez d'abord le bucket avec: python main.py create_bucket")
        else:
            print(f"❌ Erreur lors du traitement du pipeline:")
            print(f"   {e.response.get('Error', {}).get('Message', str(e))}")
        # Nettoyage en cas d'erreur
        for file in [local_file, local_parquet]:
            if os.path.exists(file):
                os.remove(file)
        raise
    except ImportError as e:
        if "pyarrow" in str(e).lower() or "parquet" in str(e).lower():
            print(f"❌ Erreur : pyarrow n'est pas installé.")
            print(f"   Le format Parquet nécessite pyarrow.")
            print(f"   Installez-le avec : pip install pyarrow")
        raise
    except Exception as e:
        print(f"❌ Erreur lors du traitement:")
        print(f"   {str(e)}")
        # Nettoyage en cas d'erreur
        for file in [local_file, local_parquet]:
            if os.path.exists(file):
                os.remove(file)
        raise


def list_bucket(bucket_name, prefix=""):
    """
    Liste les objets dans un bucket MinIO avec un préfixe optionnel.
    
    Args:
        bucket_name (str): Nom du bucket
        prefix (str): Préfixe pour filtrer les objets (optionnel)
    
    Returns:
        None
    
    Raises:
        ClientError: En cas d'erreur
    """
    global s3
    try:
        resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        contents = resp.get("Contents", [])
        if contents:
            print(f"Objets dans {bucket_name} (préfixe: '{prefix}'):")
            for obj in contents:
                print(" -", obj["Key"])
        else:
            print(f"Aucun objet trouvé dans {bucket_name} avec le préfixe '{prefix}'.")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchBucket':
            print(f"❌ Le bucket {bucket_name} n'existe pas.")
            print("   Créez d'abord le bucket avec: python main.py create_bucket")
        else:
            print(f"❌ Erreur lors de la liste des objets:")
            print(f"   {e.response.get('Error', {}).get('Message', str(e))}")
        raise


if __name__ == "__main__":
    global s3
    
    parser = argparse.ArgumentParser(description="Pipeline de traitement IoT pour GreenFarm Data Lake (MinIO)")
    parser.add_argument(
        "action",
        choices=["create_bucket", "upload_file", "process_pipeline", "list_bucket", "all"],
        help="Action à exécuter: create_bucket, upload_file, process_pipeline, list_bucket, ou all"
    )
    parser.add_argument(
        "--bucket",
        default="greenfarm-datalake-demo-datalex",
        help="Nom du bucket (défaut: greenfarm-datalake-demo-datalex). Remplacez 'datalex' par vos initiales"
    )
    parser.add_argument(
        "--file",
        default=os.path.join("src", "iot.json"),
        help="Chemin local du fichier à uploader (défaut: src/iot.json)"
    )
    parser.add_argument(
        "--s3-key",
        default="raw/current/iot.json",
        help="Chemin de destination (défaut: raw/current/iot.json)"
    )
    parser.add_argument(
        "--raw-key",
        default="raw/current/iot.json",
        help="Clé du fichier brut à traiter (défaut: raw/current/iot.json)"
    )
    parser.add_argument(
        "--processed-key",
        default=None,
        help="Clé de destination pour le fichier transformé (défaut: processed/iot.parquet)"
    )
    parser.add_argument(
        "--prefix",
        default="raw/",
        help="Préfixe pour filtrer les objets (défaut: raw/)"
    )
    parser.add_argument(
        "--endpoint",
        default="http://localhost:9000",
        help="URL de l'endpoint MinIO (défaut: http://localhost:9000)"
    )
    parser.add_argument(
        "--access-key",
        default="minioadmin",
        help="Access Key MinIO (défaut: minioadmin)"
    )
    parser.add_argument(
        "--secret-key",
        default="minioadmin",
        help="Secret Key MinIO (défaut: minioadmin)"
    )
    parser.add_argument(
        "--use-ssl",
        action="store_true",
        help="Utiliser SSL/TLS pour la connexion MinIO (défaut: False)"
    )
    
    args = parser.parse_args()
    
    # Initialisation du client MinIO (compatible S3)
    s3 = boto3.client(
        's3',
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        use_ssl=args.use_ssl,
        verify=False  # Désactiver la vérification SSL pour les instances locales
    )
    print(f"Connexion à MinIO: {args.endpoint}")
    
    if args.action == "create_bucket":
        create_bucket(args.bucket)
    elif args.action == "upload_file":
        upload_file(args.bucket, args.file, args.s3_key)
    elif args.action == "process_pipeline":
        process_iot_pipeline(args.bucket, args.raw_key, args.processed_key)
    elif args.action == "list_bucket":
        list_bucket(args.bucket, prefix=args.prefix)
    elif args.action == "all":
        # Exécution de tous les blocs
        create_bucket(args.bucket)
        upload_file(args.bucket, args.file, args.s3_key)
        process_iot_pipeline(args.bucket, args.raw_key, args.processed_key)
        list_bucket(args.bucket, prefix=args.prefix)
