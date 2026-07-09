# Importa as bibliotecas necessárias
import boto3  # controle remoto da AWS
import os  # para ler variáveis do sistema
from dotenv import load_dotenv  # para ler o arquivo .env

# Carrega as credenciais do arquivo .env
load_dotenv()

# Cria a conexão com o S3
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# Tenta listar as pastas do bucket
try:
    response = s3.list_objects_v2(
        Bucket=os.getenv('AWS_BUCKET_NAME')
    )
    print("✅ Conexão com AWS funcionando!")
    print("\nPastas encontradas no bucket:")
    for obj in response.get('Contents', []):
        print(f"{obj['Key']}")
except Exception as e:
    print(f"Erro na conexão: {e}")