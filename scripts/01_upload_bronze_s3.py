import boto3
import os
import glob
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Lê as credenciais
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# Conecta no S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Define a pasta onde estão os CSVs
pasta_raw = "data/raw"

# Lista todos os arquivos CSV da pasta
arquivos_csv = glob.glob(f"{pasta_raw}/*.csv")

print(f"Encontrados {len(arquivos_csv)} arquivos CSV para upload")

# Faz o upload de cada arquivo para o S3
for arquivo in arquivos_csv:
    nome_arquivo = os.path.basename(arquivo)
    destino_s3 = f"bronze/{nome_arquivo}"

    print(f"Enviando {nome_arquivo} para o S3...")

    s3.upload_file(
        Filename=arquivo,
        Bucket=AWS_BUCKET_NAME,
        Key=destino_s3
    )

    print(f"✓ {nome_arquivo} enviado com sucesso!")

print("\nTodos os arquivos foram enviados para a camada Bronze no S3!")