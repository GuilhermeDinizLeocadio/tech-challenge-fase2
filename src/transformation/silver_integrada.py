# =============================================================
# CAMADA SILVER — Visão Integrada
#
# RESUMO: Lê os arquivos Parquet já processados na Silver
# (estado, município e metas), cruza os resultados com as
# metas de alfabetização e cria uma visão unificada pronta
# para alimentar a camada Gold.
# =============================================================

import boto3
import pandas as pd
import os
from io import BytesIO
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
# BLOCO 1 — FUNÇÕES DE LEITURA DE PARQUET DO S3
# =============================================================
def ler_parquet_s3(caminho):
    """
    Lê um arquivo Parquet diretamente do S3.
    Analogia: pega um produto já organizado da prateleira Silver.
    """
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    buffer = BytesIO(obj['Body'].read())
    return pd.read_parquet(buffer, engine='pyarrow')

def ler_estado():
    print('📥 Lendo estado.parquet...')
    caminho = f'silver/inep/{DATA_HOJE}/estado.parquet'
    df = ler_parquet_s3(caminho)
    print(f'   ✅ {len(df):,} linhas, {len(df.columns)} colunas')
    return df

def ler_municipio():
    print('📥 Lendo municipio.parquet...')
    caminho = f'silver/inep/{DATA_HOJE}/municipio.parquet'
    df = ler_parquet_s3(caminho)
    print(f'   ✅ {len(df):,} linhas, {len(df.columns)} colunas')
    return df

def ler_metas():
    print('📥 Lendo metas.parquet...')
    caminho = f'silver/basedosdados/{DATA_HOJE}/metas.parquet'
    df = ler_parquet_s3(caminho)
    print(f'   ✅ {len(df):,} linhas, {len(df.columns)} colunas')
    return df

# =============================================================
# BLOCO 2 — INTEGRAR ESTADOS COM METAS
# =============================================================
def integrar_estados_metas(df_estado, df_metas):
    """
    Cruza os resultados por estado com as metas estaduais.
    Responde: cada estado está atingindo sua meta?
    """
    print('🔗 Integrando estados com metas...')

    # Filtra só as metas estaduais
    metas_uf = df_metas[df_metas['nivel_geografico'] == 'ESTADO'].copy()

    # Seleciona colunas relevantes das metas
    colunas_metas = ['sigla_uf', 'ano_avaliacao', 'meta_2024',
                     'meta_2025', 'meta_2026', 'meta_2027',
                     'meta_2028', 'meta_2029', 'meta_2030']
    colunas_existentes = [c for c in colunas_metas if c in metas_uf.columns]
    metas_uf = metas_uf[colunas_existentes]

    # Cruza pelo estado e ano
    df_integrado = df_estado.merge(
        metas_uf,
        on=['sigla_uf', 'ano_avaliacao'],
        how='left',
        suffixes=('', '_meta')
    )

    # Cria coluna indicando se atingiu a meta de 2024
    if 'perc_alunos_alfabetizados' in df_integrado.columns and \
       'meta_2024' in df_integrado.columns:
        df_integrado['atingiu_meta_2024'] = (
            df_integrado['perc_alunos_alfabetizados'] >=
            df_integrado['meta_2024']
        )

    print(f'   ✅ {len(df_integrado):,} linhas integradas')
    return df_integrado

# =============================================================
# BLOCO 3 — INTEGRAR MUNICÍPIOS COM METAS
# =============================================================
def integrar_municipios_metas(df_municipio, df_metas):
    """
    Cruza os resultados por município com as metas municipais.
    Responde: cada município está atingindo sua meta?
    """
    print('🔗 Integrando municípios com metas...')

    # Filtra só as metas municipais
    metas_mun = df_metas[df_metas['nivel_geografico'] == 'MUNICIPIO'].copy()

    # Padroniza chave de join
    if 'codigo_municipio' in metas_mun.columns:
        metas_mun['codigo_municipio'] = metas_mun['codigo_municipio'].astype(str).str.strip()
    if 'codigo_municipio' in df_municipio.columns:
        df_municipio['codigo_municipio'] = df_municipio['codigo_municipio'].astype(str).str.strip()

    # Seleciona colunas relevantes
    colunas_metas = ['codigo_municipio', 'ano_avaliacao',
                     'meta_2024', 'meta_2025', 'meta_2026',
                     'meta_2027', 'meta_2028', 'meta_2029', 'meta_2030']
    colunas_existentes = [c for c in colunas_metas if c in metas_mun.columns]
    metas_mun = metas_mun[colunas_existentes]

    # Cruza pelo município e ano
    df_integrado = df_municipio.merge(
        metas_mun,
        on=['codigo_municipio', 'ano_avaliacao'],
        how='left',
        suffixes=('', '_meta')
    )

    # Cria coluna indicando se atingiu a meta de 2024
    if 'perc_alunos_alfabetizados' in df_integrado.columns and \
       'meta_2024' in df_integrado.columns:
        df_integrado['atingiu_meta_2024'] = (
            df_integrado['perc_alunos_alfabetizados'] >=
            df_integrado['meta_2024']
        )

    print(f'   ✅ {len(df_integrado):,} linhas integradas')
    return df_integrado

# =============================================================
# BLOCO 4 — SALVAR EM PARQUET NO S3
# =============================================================
def salvar_silver(df, nome):
    """
    Salva cada visão integrada em Parquet na Silver.
    """
    print(f'💾 Salvando {nome}.parquet na Silver...')
    caminho = f'silver/integrado/{DATA_HOJE}/{nome}.parquet'

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
    print('SILVER LAYER — VISÃO INTEGRADA')
    print(f'Data: {DATA_HOJE}')
    print('=' * 60)

    # Lê os parquets já processados
    df_estado    = ler_estado()
    df_municipio = ler_municipio()
    df_metas     = ler_metas()

    # Integra estados com metas
    df_estados_integrado = integrar_estados_metas(df_estado, df_metas)
    salvar_silver(df_estados_integrado, 'estados_com_metas')

    # Integra municípios com metas
    df_municipios_integrado = integrar_municipios_metas(df_municipio, df_metas)
    salvar_silver(df_municipios_integrado, 'municipios_com_metas')

    print('=' * 60)
    print('✅ INTEGRAÇÃO CONCLUÍDA!')
    print('=' * 60)