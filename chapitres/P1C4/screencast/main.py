import boto3
import argparse
from botocore.exceptions import ClientError


def list_buckets():
    """
    Liste tous les buckets MinIO existants.
    """
    global s3
    try:
        response = s3.list_buckets()
        
        print("Buckets existants :")
        if response["Buckets"]:
            for bucket in response["Buckets"]:
                print(f" - {bucket['Name']}")
        else:
            print("  (aucun bucket)")
    except ClientError as e:
        print(f"❌ Erreur lors de la liste des buckets:")
        print(f"   {e.response.get('Error', {}).get('Message', str(e))}")
        raise


def create_bucket(bucket_name):
    """
    Crée un bucket MinIO avec le nom spécifié.
    
    Args:
        bucket_name (str): Nom du bucket
    """
    global s3
    try:
        print(f"\nCréation du bucket : {bucket_name}")
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket {bucket_name} créé avec succès.")
        
        # Afficher la liste mise à jour
        print("\nNouveaux buckets :")
        list_buckets()
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'BucketAlreadyExists':
            print(f"⚠️  Le bucket {bucket_name} existe déjà.")
        elif error_code == 'BucketAlreadyOwnedByYou':
            print(f"✅ Le bucket {bucket_name} vous appartient déjà.")
        else:
            print(f"❌ Erreur lors de la création du bucket {bucket_name}:")
            print(f"   {e.response.get('Error', {}).get('Message', str(e))}")
            raise


def upload_file(bucket_name, local_file, object_key):
    """
    Upload un fichier vers un bucket MinIO.
    
    Args:
        bucket_name (str): Nom du bucket
        local_file (str): Chemin local du fichier à uploader
        object_key (str): Clé (chemin) dans le bucket
    """
    global s3
    try:
        print(f"\nUpload de {object_key} dans {bucket_name}")
        s3.upload_file(local_file, bucket_name, object_key)
        print(f"✅ Fichier uploadé avec succès.")
        
        # Lister les objets du bucket
        print("\nObjets dans le bucket :")
        response = s3.list_objects_v2(Bucket=bucket_name)
        if response.get("Contents"):
            for obj in response["Contents"]:
                print(f" - {obj['Key']}")
        else:
            print("  (aucun objet)")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchBucket':
            print(f"❌ Le bucket {bucket_name} n'existe pas.")
            print("   Créez d'abord le bucket avec: python main.py create_bucket")
        else:
            print(f"❌ Erreur lors de l'upload du fichier:")
            print(f"   {e.response.get('Error', {}).get('Message', str(e))}")
        raise
    except FileNotFoundError:
        print(f"❌ Le fichier {local_file} n'existe pas.")
        raise


if __name__ == "__main__":
    global s3
    
    parser = argparse.ArgumentParser(description="Démonstration MinIO pour screencast")
    parser.add_argument(
        "action",
        choices=["list_buckets", "create_bucket", "upload_file", "all"],
        help="Action à exécuter: list_buckets, create_bucket, upload_file, ou all"
    )
    parser.add_argument(
        "--bucket",
        default="openclassrooms-datalake-8481716-demo-xx",
        help="Nom du bucket (défaut: openclassrooms-datalake-8481716-demo-xx). Remplacez xx par vos initiales"
    )
    parser.add_argument(
        "--file",
        default="sample.txt",
        help="Chemin local du fichier à uploader (défaut: sample.txt)"
    )
    parser.add_argument(
        "--s3-key",
        default="raw/current/sample.txt",
        help="Clé de destination (défaut: raw/current/sample.txt)"
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
    
    if args.action == "list_buckets":
        list_buckets()
    elif args.action == "create_bucket":
        create_bucket(args.bucket)
    elif args.action == "upload_file":
        upload_file(args.bucket, args.file, args.s3_key)
    elif args.action == "all":
        # Exécution de toutes les étapes
        list_buckets()
        create_bucket(args.bucket)
        upload_file(args.bucket, args.file, args.s3_key)
