# =============================================================
# CAMADA BRONZE — Ingestão de dados brutos
# Objetivo: ler os CSVs locais e enviar para o S3 sem modificar
# Analogia: pegar as caixas do fornecedor e colocar na prateleira
# =============================================================

import boto3
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# Carrega as credenciais do arquivo .env
load_dotenv()

# =============================================================
# CONFIGURAÇÃO DA CONEXÃO COM AWS
# =============================================================
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

BUCKET = os.getenv('AWS_BUCKET_NAME')

# Data de hoje — usamos para registrar quando o dado foi ingerido
DATA_HOJE = datetime.now().strftime('%Y-%m-%d')

# =============================================================
# LISTA DE ARQUIVOS PARA INGERIR
# Formato: (caminho local, caminho no S3)
# =============================================================
ARQUIVOS = [
    # Arquivos do INEP
    ('data/bronze/inep/TS_ALUNO.csv',      f'bronze/inep/{DATA_HOJE}/TS_ALUNO.csv'),
    ('data/bronze/inep/TS_ESTADO.csv',     f'bronze/inep/{DATA_HOJE}/TS_ESTADO.csv'),
    ('data/bronze/inep/TS_ITEM.csv',       f'bronze/inep/{DATA_HOJE}/TS_ITEM.csv'),
    ('data/bronze/inep/TS_MUNICIPIO.csv',  f'bronze/inep/{DATA_HOJE}/TS_MUNICIPIO.csv'),
    # Arquivos da Base dos Dados
    ('data/bronze/basedosdados/meta_brasil.csv',    f'bronze/basedosdados/{DATA_HOJE}/meta_brasil.csv'),
    ('data/bronze/basedosdados/meta_municipio.csv', f'bronze/basedosdados/{DATA_HOJE}/meta_municipio.csv'),
    ('data/bronze/basedosdados/meta_uf.csv',        f'bronze/basedosdados/{DATA_HOJE}/meta_uf.csv'),
    ('data/bronze/basedosdados/municipio.csv',      f'bronze/basedosdados/{DATA_HOJE}/municipio.csv'),
    ('data/bronze/basedosdados/uf.csv',             f'bronze/basedosdados/{DATA_HOJE}/uf.csv'),
]

# =============================================================
# FUNÇÃO DE UPLOAD
# =============================================================
def upload_para_s3(caminho_local, caminho_s3):
    """
    Faz upload de um arquivo local para o S3.
    Analogia: pega a caixa do chão e coloca na prateleira.
    """
    try:
        print(f'📤 Enviando: {caminho_local}')
        s3.upload_file(caminho_local, BUCKET, caminho_s3)
        print(f'   ✅ Salvo em: s3://{BUCKET}/{caminho_s3}')
        return True
    except Exception as e:
        print(f'   ❌ Erro: {e}')
        return False

# =============================================================
# EXECUÇÃO PRINCIPAL
# =============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('INICIANDO INGESTÃO BRONZE')
    print(f'Data: {DATA_HOJE}')
    print(f'Total de arquivos: {len(ARQUIVOS)}')
    print('=' * 60)

    sucesso = 0
    falha = 0

    for caminho_local, caminho_s3 in ARQUIVOS:
        resultado = upload_para_s3(caminho_local, caminho_s3)
        if resultado:
            sucesso += 1
        else:
            falha += 1

    print('=' * 60)
    print(f'INGESTÃO CONCLUÍDA')
    print(f'✅ Sucessos: {sucesso}')
    print(f'❌ Falhas:   {falha}')
    print('=' * 60)