import boto3
import os
import argparse
import datetime
from botocore.exceptions import ClientError
import pandas as pd


def create_bucket(bucket_name):
    """
    Crée un bucket S3 avec le nom spécifié.
    
    Args:
        bucket_name (str): Nom du bucket (doit être unique globalement dans AWS)
    
    Returns:
        None
    
    Raises:
        ClientError: En cas d'erreur AWS (bucket existe déjà, conflit, etc.)
    """
    global s3
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} créé avec succès.")
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
    Upload un fichier vers un bucket S3.
    
    Args:
        bucket_name (str): Nom du bucket S3
        local_file_path (str): Chemin local du fichier à uploader
        s3_key (str): Clé (chemin) dans le bucket S3
    
    Returns:
        None
    
    Raises:
        FileNotFoundError: Si le fichier local n'existe pas
        ClientError: En cas d'erreur AWS
    """
    global s3
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Le fichier {local_file_path} n'existe pas.")
    
    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Fichier {local_file_path} envoyé dans {s3_key}.")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchBucket':
            print(f"❌ Le bucket {bucket_name} n'existe pas.")
            print("   Créez d'abord le bucket avec: python main.py create_bucket")
        else:
            print(f"❌ Erreur lors de l'upload du fichier:")
            print(f"   {e.response.get('Error', {}).get('Message', str(e))}")
        raise


def list_bucket(bucket_name, prefix=""):
    """
    Liste les objets dans un bucket S3 avec un préfixe optionnel.
    
    Args:
        bucket_name (str): Nom du bucket S3
        prefix (str): Préfixe pour filtrer les objets (optionnel)
    
    Returns:
        None
    
    Raises:
        ClientError: En cas d'erreur AWS
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


def process_pipeline(bucket_name, raw_key, processed_key=None):
    """
    Traite un fichier brut du Data Lake : télécharge, transforme, sauvegarde en Parquet et archive.
    
    Le pipeline effectue les étapes suivantes :
    1. Télécharge le fichier CSV depuis raw/current/
    2. Lit et valide le contenu avec pandas
    3. Transforme les données (suppression des NaN)
    4. Sauvegarde en Parquet dans processed/
    5. Archive le fichier brut dans raw/archived/ avec timestamp
    6. Supprime le fichier de raw/current/
    
    Args:
        bucket_name (str): Nom du bucket S3
        raw_key (str): Clé S3 du fichier brut dans raw/current/ (ex: "raw/current/ventes.csv")
        processed_key (str, optionnel): Clé S3 de destination dans processed/ 
                                       (par défaut: "processed/ventes.parquet")
    
    Returns:
        None
    
    Raises:
        ClientError: En cas d'erreur AWS
        FileNotFoundError: Si le fichier brut n'existe pas dans S3
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
    base_name = os.path.splitext(filename)[0]  # "ventes" sans extension
    
    if processed_key is None:
        processed_key = f"processed/{base_name}.parquet"
    
    local_csv = filename  # "ventes.csv"
    local_parquet = f"{base_name}.parquet"  # "ventes.parquet"
    
    try:
        print(f"🔄 Démarrage du pipeline de traitement...")
        print(f"   Fichier source: {raw_key}")
        
        # Étape 1 : Télécharger le fichier brut
        print(f"\nÉtape 1 : Téléchargement du fichier brut...")
        s3.download_file(bucket_name, raw_key, local_csv)
        print(f"   ✅ Fichier téléchargé: {local_csv}")
        
        # Étape 2 : Lire et valider le fichier
        print(f"\nÉtape 2 : Lecture et validation du fichier...")
        df = pd.read_csv(local_csv)
        print(f"   ✅ Fichier lu avec succès")
        print(f"   Dimensions: {df.shape[0]} lignes, {df.shape[1]} colonnes")
        print(f"\n   Aperçu des données:")
        print(df.head().to_string())
        
        # Étape 3 : Transformer les données
        print(f"\nÉtape 3 : Transformation des données...")
        initial_rows = len(df)
        df = df.dropna()
        removed_rows = initial_rows - len(df)
        print(f"   ✅ Transformation terminée")
        if removed_rows > 0:
            print(f"   🗑️  {removed_rows} ligne(s) avec valeurs manquantes supprimée(s)")
        else:
            print(f"   ℹ️  Aucune ligne supprimée (pas de valeurs manquantes)")
        print(f"   Dimensions finales: {df.shape[0]} lignes, {df.shape[1]} colonnes")
        
        # Étape 4 : Sauvegarder en Parquet
        print(f"\nÉtape 4 : Sauvegarde en format Parquet...")
        df.to_parquet(local_parquet)
        s3.upload_file(local_parquet, bucket_name, processed_key)
        print(f"   ✅ Fichier transformé déposé dans: {processed_key}")
        
        # Étape 5 : Archiver le fichier brut
        print(f"\nÉtape 5 : Archivage du fichier brut...")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_key = f"raw/archived/{base_name}_{timestamp}.csv"
        
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
        if os.path.exists(local_csv):
            os.remove(local_csv)
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
        for file in [local_csv, local_parquet]:
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
        for file in [local_csv, local_parquet]:
            if os.path.exists(file):
                os.remove(file)
        raise


if __name__ == "__main__":
    # Déclaration globale du client S3
    global s3
    
    parser = argparse.ArgumentParser(description="Gestion de fichiers avec AWS S3")
    parser.add_argument(
        "action",
        choices=["create_bucket", "upload_file", "list_bucket", "process_pipeline", "all"],
        help="Action à exécuter: create_bucket, upload_file, list_bucket, process_pipeline, ou all"
    )
    parser.add_argument(
        "--bucket",
        default="openclassrooms-datalake-8481716",
        help="Nom du bucket S3 (défaut: openclassrooms-datalake-8481716)"
    )
    parser.add_argument(
        "--file",
        default=os.path.join("src", "ventes.csv"),
        help="Chemin local du fichier à uploader (défaut: src/ventes.csv)"
    )
    parser.add_argument(
        "--s3-key",
        default="raw/current/ventes.csv",
        help="Chemin de destination dans S3 (défaut: raw/current/ventes.csv)"
    )
    parser.add_argument(
        "--prefix",
        default="raw/",
        help="Préfixe pour filtrer les objets (défaut: raw/)"
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Nom du profil AWS à utiliser (optionnel, utilise les credentials par défaut si non spécifié)"
    )
    parser.add_argument(
        "--raw-key",
        default="raw/current/ventes.csv",
        help="Clé S3 du fichier brut à traiter (défaut: raw/current/ventes.csv)"
    )
    parser.add_argument(
        "--processed-key",
        default=None,
        help="Clé S3 de destination pour le fichier transformé (défaut: processed/ventes.parquet)"
    )
    
    args = parser.parse_args()
    
    # Initialisation du client S3 avec ou sans profil
    if args.profile:
        session = boto3.Session(profile_name=args.profile)
        s3 = session.client('s3')
        print(f"Utilisation du profil AWS: {args.profile}")
    else:
        s3 = boto3.client('s3')
    
    if args.action == "create_bucket":
        create_bucket(args.bucket)
    elif args.action == "upload_file":
        upload_file(args.bucket, args.file, args.s3_key)
    elif args.action == "list_bucket":
        list_bucket(args.bucket, prefix=args.prefix)
    elif args.action == "process_pipeline":
        process_pipeline(args.bucket, args.raw_key, args.processed_key)
    elif args.action == "all":
        # Exécution de tous les blocs
        create_bucket(args.bucket)
        upload_file(args.bucket, args.file, args.s3_key)
        list_bucket(args.bucket, prefix=args.prefix)