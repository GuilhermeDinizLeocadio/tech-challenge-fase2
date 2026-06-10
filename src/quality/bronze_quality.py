# =============================================================
# QUALIDADE DE DADOS — Camada Bronze
# 
# RESUMO: Este script lê os 9 arquivos do S3, verifica a 
# qualidade de cada um e gera um relatório com problemas 
# encontrados. Pensa como um inspetor de qualidade que confere
# cada caixa antes de deixar entrar no estoque.
# =============================================================

import boto3
import pandas as pd
import os
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# =============================================================
# CONEXÃO COM AWS
# =============================================================
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

BUCKET = os.getenv('AWS_BUCKET_NAME')
DATA_HOJE = datetime.now().strftime('%Y-%m-%d')

# =============================================================
# ARQUIVOS PARA VALIDAR
# Formato: (caminho no S3, separador, encoding)
# =============================================================
ARQUIVOS = {
    'TS_ALUNO':      (f'bronze/inep/{DATA_HOJE}/TS_ALUNO.csv',      ';', 'latin-1'),
    'TS_ESTADO':     (f'bronze/inep/{DATA_HOJE}/TS_ESTADO.csv',     ';', 'latin-1'),
    'TS_ITEM':       (f'bronze/inep/{DATA_HOJE}/TS_ITEM.csv',       ';', 'latin-1'),
    'TS_MUNICIPIO':  (f'bronze/inep/{DATA_HOJE}/TS_MUNICIPIO.csv',  ';', 'latin-1'),
    'meta_brasil':   (f'bronze/basedosdados/{DATA_HOJE}/meta_brasil.csv',   ',', 'utf-8'),
    'meta_municipio':(f'bronze/basedosdados/{DATA_HOJE}/meta_municipio.csv',',', 'utf-8'),
    'meta_uf':       (f'bronze/basedosdados/{DATA_HOJE}/meta_uf.csv',       ',', 'utf-8'),
    'municipio':     (f'bronze/basedosdados/{DATA_HOJE}/municipio.csv',     ',', 'utf-8'),
    'uf':            (f'bronze/basedosdados/{DATA_HOJE}/uf.csv',            ',', 'utf-8'),
}

# =============================================================
# FUNÇÃO: LER ARQUIVO DO S3
# =============================================================
def ler_s3(caminho, separador, encoding):
    """
    Lê um arquivo CSV direto do S3 e retorna um DataFrame.
    Pensa como pegar uma caixa da prateleira e abrir para inspecionar.
    """
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=caminho)
        conteudo = obj['Body'].read().decode(encoding)
        df = pd.read_csv(StringIO(conteudo), sep=separador, low_memory=False)
        return df, None
    except Exception as e:
        return None, str(e)

# =============================================================
# FUNÇÃO: VALIDAR QUALIDADE
# =============================================================
def validar_qualidade(nome, df):
    """
    Executa 4 verificações de qualidade em cada arquivo.
    Retorna um dicionário com os resultados.
    """
    resultado = {
        'arquivo': nome,
        'total_linhas': len(df),
        'total_colunas': len(df.columns),
        'duplicatas': 0,
        'valores_ausentes': {},
        'colunas_com_null': 0,
        'status': '✅ OK'
    }

    # Verificação 1 — Duplicatas
    # Conta quantas linhas são idênticas
    duplicatas = df.duplicated().sum()
    resultado['duplicatas'] = int(duplicatas)
    if duplicatas > 0:
        resultado['status'] = '⚠️ ATENÇÃO'

    # Verificação 2 — Valores ausentes
    # Conta quantos valores faltam em cada coluna
    nulos = df.isnull().sum()
    colunas_com_null = nulos[nulos > 0]
    resultado['colunas_com_null'] = len(colunas_com_null)
    if len(colunas_com_null) > 0:
        resultado['valores_ausentes'] = colunas_com_null.to_dict()
        resultado['status'] = '⚠️ ATENÇÃO'

    return resultado

# =============================================================
# EXECUÇÃO PRINCIPAL
# =============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('RELATÓRIO DE QUALIDADE — CAMADA BRONZE')
    print(f'Data: {DATA_HOJE}')
    print('=' * 60)

    resultados = []

    for nome, (caminho, sep, enc) in ARQUIVOS.items():
        print(f'\n📋 Validando: {nome}')
        df, erro = ler_s3(caminho, sep, enc)

        if erro:
            print(f'   ❌ Erro ao ler arquivo: {erro}')
            continue

        resultado = validar_qualidade(nome, df)
        resultados.append(resultado)

        print(f'   {resultado["status"]}')
        print(f'   Linhas: {resultado["total_linhas"]:,}')
        print(f'   Colunas: {resultado["total_colunas"]}')
        print(f'   Duplicatas: {resultado["duplicatas"]:,}')
        print(f'   Colunas com valores ausentes: {resultado["colunas_com_null"]}')

        if resultado['valores_ausentes']:
            print('   Detalhes dos valores ausentes:')
            for col, qtd in resultado['valores_ausentes'].items():
                print(f'      → {col}: {qtd:,} valores ausentes')

    print('\n' + '=' * 60)
    print('RESUMO FINAL')
    print('=' * 60)
    ok = sum(1 for r in resultados if r['status'] == '✅ OK')
    atencao = sum(1 for r in resultados if r['status'] == '⚠️ ATENÇÃO')
    print(f'✅ Arquivos OK:      {ok}')
    print(f'⚠️  Arquivos atenção: {atencao}')
    print('=' * 60)