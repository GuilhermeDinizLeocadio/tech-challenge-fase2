# =============================================================
# CAMADA SILVER — Transformação do TS_ESTADO
#
# RESUMO: Lê o TS_ESTADO.csv da camada Bronze no S3, realiza
# limpeza e padronização dos dados, e salva em formato Parquet
# na camada Silver do S3.
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
DATA_BRONZE = '2026-06-10'  # data em que fizemos a ingestão Bronze
DATA_HOJE = datetime.now().strftime('%Y-%m-%d')

# =============================================================
# BLOCO 1 — LER ARQUIVO BRONZE DO S3
# =============================================================
def ler_bronze():
    """
    Lê o TS_ESTADO.csv da camada Bronze.
    Analogia: pega a caixa da prateleira Bronze para inspecionar.
    """
    print('📥 Lendo TS_ESTADO.csv da camada Bronze...')
    caminho = f'bronze/inep/{DATA_BRONZE}/TS_ESTADO.csv'
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    conteudo = obj['Body'].read().decode('latin-1')
    df = pd.read_csv(StringIO(conteudo), sep=';', low_memory=False)
    print(f'   ✅ Lido! {len(df)} linhas, {len(df.columns)} colunas')
    return df

# =============================================================
# BLOCO 2 — PADRONIZAR NOMES DAS COLUNAS
# =============================================================
def padronizar_colunas(df):
    """
    Renomeia as colunas para nomes mais claros e em português.
    Analogia: trocar etiquetas técnicas por etiquetas legíveis.
    """
    print('🏷️  Padronizando nomes das colunas...')
    mapeamento = {
        'NU_ANO_AVALIACAO':        'ano_avaliacao',
        'CO_UF':                   'codigo_uf',
        'SG_UF':                   'sigla_uf',
        'NO_UF':                   'nome_uf',
        'CO_REGIAO':               'codigo_regiao',
        'NO_REGIAO':               'nome_regiao',
        'TP_DEPENDENCIA':          'tipo_dependencia',
        'QT_ALUNO_MATRICULADO':    'qtd_alunos_matriculados',
        'QT_ALUNO_AVALIADO':       'qtd_alunos_avaliados',
        'QT_ALUNO_ALFABETIZADO':   'qtd_alunos_alfabetizados',
        'PC_ALUNO_ALFABETIZADO':   'perc_alunos_alfabetizados',
        'VL_MEDIA_LP':             'media_lingua_portuguesa',
        'PC_NIVEL_0':              'perc_nivel_0',
        'PC_NIVEL_1':              'perc_nivel_1',
        'PC_NIVEL_2':              'perc_nivel_2',
        'PC_NIVEL_3':              'perc_nivel_3',
    }
    # Renomeia só as colunas que existem no DataFrame
    colunas_existentes = {k: v for k, v in mapeamento.items() if k in df.columns}
    df = df.rename(columns=colunas_existentes)
    print(f'   ✅ {len(colunas_existentes)} colunas renomeadas')
    return df

# =============================================================
# BLOCO 3 — LIMPAR E TRATAR OS DADOS
# =============================================================
def limpar_dados(df):
    """
    Realiza limpeza geral dos dados.
    Analogia: tirar a poeira e organizar os itens da caixa.
    """
    print('🧹 Limpando e tratando os dados...')

    # Remove espaços extras em colunas de texto
    colunas_texto = df.select_dtypes(include='object').columns
    for col in colunas_texto:
        df[col] = df[col].str.strip()

    # Garante que ano_avaliacao é inteiro
    if 'ano_avaliacao' in df.columns:
        df['ano_avaliacao'] = df['ano_avaliacao'].astype(int)

    # Garante que percentuais são decimais
    colunas_perc = [c for c in df.columns if 'perc_' in c or 'media_' in c]
    for col in colunas_perc:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Adiciona coluna de controle — quando esse dado foi processado
    df['data_processamento'] = DATA_HOJE

    print(f'   ✅ Dados limpos! {len(df)} linhas mantidas')
    return df

# =============================================================
# BLOCO 4 — SALVAR EM PARQUET NO S3
# =============================================================
def salvar_silver(df):
    """
    Salva o DataFrame limpo em formato Parquet na camada Silver.
    Analogia: embala o produto organizado numa embalagem eficiente
    e coloca na prateleira Silver.
    Parquet é como um ZIP inteligente — comprime mas lê rápido.
    """
    print('💾 Salvando na camada Silver em formato Parquet...')
    caminho = f'silver/inep/{DATA_HOJE}/estado.parquet'

    # Converte DataFrame para Parquet em memória
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    # Envia para o S3
    s3.put_object(
        Bucket=BUCKET,
        Key=caminho,
        Body=buffer.getvalue()
    )
    print(f'   ✅ Salvo em: s3://{BUCKET}/{caminho}')
    print(f'   📊 {len(df)} linhas, {len(df.columns)} colunas')

# =============================================================
# EXECUÇÃO PRINCIPAL
# =============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('SILVER LAYER — TS_ESTADO')
    print(f'Data: {DATA_HOJE}')
    print('=' * 60)

    df = ler_bronze()
    df = padronizar_colunas(df)
    df = limpar_dados(df)
    salvar_silver(df)

    print('=' * 60)
    print('✅ TRANSFORMAÇÃO CONCLUÍDA!')
    print('=' * 60)