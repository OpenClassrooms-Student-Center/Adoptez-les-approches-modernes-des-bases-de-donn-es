import boto3
import argparse
from botocore.exceptions import ClientError


def list_buckets():
    """
    Liste tous les buckets S3 existants.
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
    Crée un bucket S3 avec le nom spécifié.
    
    Args:
        bucket_name (str): Nom du bucket (doit être unique globalement dans AWS)
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
    Upload un fichier vers un bucket S3.
    
    Args:
        bucket_name (str): Nom du bucket S3
        local_file (str): Chemin local du fichier à uploader
        object_key (str): Clé (chemin) dans le bucket S3
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
    
    parser = argparse.ArgumentParser(description="Démonstration AWS S3 pour screencast")
    parser.add_argument(
        "action",
        choices=["list_buckets", "create_bucket", "upload_file", "all"],
        help="Action à exécuter: list_buckets, create_bucket, upload_file, ou all"
    )
    parser.add_argument(
        "--bucket",
        default="openclassrooms-datalake-8481716-demo-datalex",
        help="Nom du bucket S3 (défaut: openclassrooms-datalake-8481716-demo-datalex). Remplacez datalex par vos initiales"
    )
    parser.add_argument(
        "--file",
        default="sample.txt",
        help="Chemin local du fichier à uploader (défaut: sample.txt)"
    )
    parser.add_argument(
        "--s3-key",
        default="raw/current/sample.txt",
        help="Clé S3 de destination (défaut: raw/current/sample.txt)"
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Nom du profil AWS à utiliser (optionnel, utilise les credentials par défaut si non spécifié)"
    )
    
    args = parser.parse_args()
    
    # Initialisation du client S3 avec ou sans profil
    if args.profile:
        session = boto3.Session(profile_name=args.profile)
        s3 = session.client('s3')
        print(f"Utilisation du profil AWS: {args.profile}")
    else:
        s3 = boto3.client('s3')
    
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
