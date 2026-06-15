# =============================================================
# CAMADA SILVER — Transformação das Metas de Alfabetização
#
# RESUMO: Lê as 3 tabelas de metas (Brasil, UF e Município)
# da camada Bronze, limpa e padroniza cada uma, empilha
# numa tabela única com coluna de nível geográfico e salva
# em Parquet na camada Silver do S3.
# =============================================================

import boto3
import pandas as pd
import os
from io import StringIO, BytesIO
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
DATA_BRONZE = '2026-06-10'
DATA_HOJE = datetime.now().strftime('%Y-%m-%d')

# =============================================================
# BLOCO 1 — FUNÇÕES DE LEITURA
# =============================================================
def ler_csv_s3(caminho, sep=',', encoding='utf-8'):
    """
    Função genérica para ler qualquer CSV do S3.
    Reutilizamos ela para as 3 tabelas de metas.
    """
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    conteudo = obj['Body'].read().decode(encoding)
    return pd.read_csv(StringIO(conteudo), sep=sep, low_memory=False)

def ler_metas_brasil():
    print('📥 Lendo meta_brasil.csv...')
    caminho = f'bronze/basedosdados/{DATA_BRONZE}/meta_brasil.csv'
    df = ler_csv_s3(caminho)
    df['nivel_geografico'] = 'BRASIL'
    df['codigo_geografico'] = 'BR'
    df['nome_geografico'] = 'Brasil'
    print(f'   ✅ {len(df):,} linhas')
    return df

def ler_metas_uf():
    print('📥 Lendo meta_uf.csv...')
    caminho = f'bronze/basedosdados/{DATA_BRONZE}/meta_uf.csv'
    df = ler_csv_s3(caminho)
    df['nivel_geografico'] = 'ESTADO'
    print(f'   ✅ {len(df):,} linhas')
    return df

def ler_metas_municipio():
    print('📥 Lendo meta_municipio.csv...')
    caminho = f'bronze/basedosdados/{DATA_BRONZE}/meta_municipio.csv'
    df = ler_csv_s3(caminho)
    df['nivel_geografico'] = 'MUNICIPIO'
    print(f'   ✅ {len(df):,} linhas')
    return df

# =============================================================
# BLOCO 2 — PADRONIZAR COLUNAS
# =============================================================
def padronizar_colunas(df):
    """
    Padroniza os nomes das colunas que existem em cada tabela.
    Como cada tabela tem colunas diferentes, renomeamos
    só as que existem em cada uma.
    """
    mapeamento = {
        'ano':                      'ano_avaliacao',
        'sigla_uf':                 'sigla_uf',
        'id_municipio':             'codigo_municipio',
        'nome_municipio':           'nome_municipio',
        'rede':                     'rede_ensino',
        'taxa_alfabetizacao':       'taxa_alfabetizacao',
        'meta_alfabetizacao_2024':  'meta_2024',
        'meta_alfabetizacao_2025':  'meta_2025',
        'meta_alfabetizacao_2026':  'meta_2026',
        'meta_alfabetizacao_2027':  'meta_2027',
        'meta_alfabetizacao_2028':  'meta_2028',
        'meta_alfabetizacao_2029':  'meta_2029',
        'meta_alfabetizacao_2030':  'meta_2030',
        'nivel_alfabetizacao':      'nivel_alfabetizacao',
        'percentual_participacao':  'percentual_participacao',
    }
    colunas_existentes = {k: v for k, v in mapeamento.items() if k in df.columns}
    df = df.rename(columns=colunas_existentes)
    return df

# =============================================================
# BLOCO 3 — LIMPAR OS DADOS
# =============================================================
def limpar_dados(df):
    """
    Trata valores ausentes e padroniza tipos.
    """
    # Colunas numéricas — ausentes viram -1
    colunas_num = [c for c in df.columns if any(
        x in c for x in ['taxa_', 'meta_', 'percentual_']
    )]
    for col in colunas_num:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1)

    # Colunas de texto — ausentes viram 'NAO_INFORMADO'
    colunas_texto = df.select_dtypes(include='object').columns
    for col in colunas_texto:
        df[col] = df[col].fillna('NAO_INFORMADO').str.strip()

    return df

# =============================================================
# BLOCO 4 — EMPILHAR AS TRÊS TABELAS
# =============================================================
def empilhar_metas(df_brasil, df_uf, df_municipio):
    """
    Junta as três tabelas numa só usando pd.concat.
    Analogia: empilhar três planilhas numa só,
    mantendo uma coluna que diz de qual planilha
    cada linha veio.
    """
    print('📚 Empilhando as 3 tabelas de metas...')

    # Padroniza colunas de cada uma
    df_brasil   = padronizar_colunas(df_brasil)
    df_uf       = padronizar_colunas(df_uf)
    df_municipio = padronizar_colunas(df_municipio)

    # Limpa cada uma
    df_brasil    = limpar_dados(df_brasil)
    df_uf        = limpar_dados(df_uf)
    df_municipio = limpar_dados(df_municipio)

    # Empilha — cada linha sabe de qual nível veio
    df_final = pd.concat(
        [df_brasil, df_uf, df_municipio],
        ignore_index=True,  # reinicia o índice
        sort=False          # mantém a ordem das colunas
    )

    # Adiciona coluna de controle
    df_final['data_processamento'] = DATA_HOJE

    print(f'   ✅ Total empilhado: {len(df_final):,} linhas')
    print(f'   📊 Colunas: {len(df_final.columns)}')
    return df_final

# =============================================================
# BLOCO 5 — SALVAR EM PARQUET NO S3
# =============================================================
def salvar_silver(df):
    """
    Salva a tabela unificada de metas em Parquet na Silver.
    """
    print('💾 Salvando na camada Silver em formato Parquet...')
    caminho = f'silver/basedosdados/{DATA_HOJE}/metas.parquet'

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    s3.put_object(
        Bucket=BUCKET,
        Key=caminho,
        Body=buffer.getvalue()
    )
    print(f'   ✅ Salvo em: s3://{BUCKET}/{caminho}')
    print(f'   📊 {len(df):,} linhas, {len(df.columns)} colunas')

# =============================================================
# EXECUÇÃO PRINCIPAL
# =============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('SILVER LAYER — METAS DE ALFABETIZAÇÃO')
    print(f'Data: {DATA_HOJE}')
    print('=' * 60)

    # Lê as 3 fontes
    df_brasil    = ler_metas_brasil()
    df_uf        = ler_metas_uf()
    df_municipio = ler_metas_municipio()

    # Empilha e limpa
    df_final = empilhar_metas(df_brasil, df_uf, df_municipio)

    # Salva
    salvar_silver(df_final)

    print('=' * 60)
    print('✅ TRANSFORMAÇÃO CONCLUÍDA!')
    print('=' * 60)