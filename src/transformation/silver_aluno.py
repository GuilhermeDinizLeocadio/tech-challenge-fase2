# =============================================================
# CAMADA SILVER — Transformação dos dados de Aluno
#
# RESUMO: Lê o TS_ALUNO.csv da camada Bronze (2.2M linhas),
# realiza limpeza, padronização e tratamento de valores
# ausentes, e salva em Parquet na camada Silver do S3.
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
# BLOCO 1 — LER ARQUIVO BRONZE DO S3
# =============================================================
def ler_bronze():
    """
    Lê o TS_ALUNO.csv da camada Bronze.
    Atenção: arquivo grande! 2.2 milhões de linhas.
    """
    print('📥 Lendo TS_ALUNO.csv da camada Bronze...')
    print('   ⏳ Aguarde — arquivo grande (257MB)...')
    caminho = f'bronze/inep/{DATA_BRONZE}/TS_ALUNO.csv'
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    conteudo = obj['Body'].read().decode('latin-1')
    df = pd.read_csv(
        StringIO(conteudo),
        sep=';',
        low_memory=False
    )
    print(f'   ✅ Lido! {len(df):,} linhas, {len(df.columns)} colunas')
    return df

# =============================================================
# BLOCO 2 — PADRONIZAR NOMES DAS COLUNAS
# =============================================================
def padronizar_colunas(df):
    """
    Renomeia as colunas para nomes mais claros em português.
    """
    print('🏷️  Padronizando nomes das colunas...')
    mapeamento = {
        'NU_ANO_AVALIACAO':    'ano_avaliacao',
        'CO_UF':               'codigo_uf',
        'SG_UF':               'sigla_uf',
        'ID_ALUNO':            'id_aluno',
        'TP_SERIE':            'tipo_serie',
        'ID_ESCOLA':           'id_escola',
        'TP_DEPENDENCIA':      'tipo_dependencia',
        'CO_MUNICIPIO':        'codigo_municipio',
        'NO_MUNICIPIO':        'nome_municipio',
        'IN_PRESENCA_LP':      'presenca_lingua_portuguesa',
        'IN_PREENCHIMENTO_LP': 'preenchimento_lingua_portuguesa',
        'CO_CADERNO_LP':       'codigo_caderno',
        'CO_BLOCO_1':          'codigo_bloco_1',
        'TX_RESPOSTA_BLOCO_1': 'resposta_bloco_1',
        'TX_GABARITO_BLOCO_1': 'gabarito_bloco_1',
        'CO_BLOCO_2':          'codigo_bloco_2',
        'TX_RESPOSTA_BLOCO_2': 'resposta_bloco_2',
        'TX_GABARITO_BLOCO_2': 'gabarito_bloco_2',
        'CO_BLOCO_3':          'codigo_bloco_3',
        'TX_RESPOSTA_BLOCO_3': 'resposta_bloco_3',
        'TX_GABARITO_BLOCO_3': 'gabarito_bloco_3',
        'CO_BLOCO_4':          'codigo_bloco_4',
        'TX_RESPOSTA_BLOCO_4': 'resposta_bloco_4',
        'TX_GABARITO_BLOCO_4': 'gabarito_bloco_4',
        'VL_PESO_ALUNO_LP':    'peso_aluno',
        'VL_PROFICIENCIA_LP':  'proficiencia_lingua_portuguesa',
        'IN_ALFABETIZADO':     'alfabetizado',
    }
    colunas_existentes = {k: v for k, v in mapeamento.items() if k in df.columns}
    df = df.rename(columns=colunas_existentes)
    print(f'   ✅ {len(colunas_existentes)} colunas renomeadas')
    return df

# =============================================================
# BLOCO 3 — TRATAR VALORES AUSENTES
# =============================================================
def tratar_ausentes(df):
    """
    Trata valores ausentes com estratégias adequadas
    para cada tipo de coluna.
    Analogia: preencher campos em branco de um formulário
    com o valor mais adequado para cada situação.
    """
    print('🔧 Tratando valores ausentes...')

    # Escolas sem código — documentado pelo INEP (628 casos)
    # Preenchemos com 'NAO_IDENTIFICADO'
    if 'id_escola' in df.columns:
         df['id_escola'] = pd.to_numeric(df['id_escola'], errors='coerce').fillna(0).astype(int)

    if 'codigo_municipio' in df.columns:
        df['codigo_municipio'] = df['codigo_municipio'].fillna(0).astype(int)

    if 'nome_municipio' in df.columns:
        df['nome_municipio'] = df['nome_municipio'].fillna('NAO_IDENTIFICADO')

    if 'tipo_dependencia' in df.columns:
        df['tipo_dependencia'] = df['tipo_dependencia'].fillna(0).astype(int)

    # Blocos ausentes — aluno não recebeu esse bloco
    # Preenchemos com 'SEM_BLOCO'
    colunas_bloco = [c for c in df.columns if 'bloco' in c]
    for col in colunas_bloco:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna('SEM_BLOCO')
        else:
            df[col] = df[col].fillna(0)

    # Proficiência ausente — aluno faltou ou não preencheu
    # Mantemos como -1 para indicar ausência (não confundir com zero)
    if 'proficiencia_lingua_portuguesa' in df.columns:
        df['proficiencia_lingua_portuguesa'] = \
            df['proficiencia_lingua_portuguesa'].fillna(-1)

    if 'peso_aluno' in df.columns:
        df['peso_aluno'] = df['peso_aluno'].fillna(0)

    print(f'   ✅ Valores ausentes tratados!')
    return df

# =============================================================
# BLOCO 4 — PADRONIZAR TIPOS E LIMPAR
# =============================================================
def limpar_dados(df):
    """
    Padroniza tipos de dados e remove espaços extras.
    """
    print('🧹 Limpando e padronizando tipos...')

    # Garante que ano é inteiro
    if 'ano_avaliacao' in df.columns:
        df['ano_avaliacao'] = df['ano_avaliacao'].astype(int)

    # Padroniza texto — remove espaços extras
    colunas_texto = ['sigla_uf', 'nome_municipio', 'id_aluno']
    for col in colunas_texto:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # Cria coluna indicando se aluno tem proficiência válida
    if 'proficiencia_lingua_portuguesa' in df.columns:
        df['tem_proficiencia'] = df['proficiencia_lingua_portuguesa'] > 0

    # Adiciona coluna de controle
    df['data_processamento'] = DATA_HOJE

    print(f'   ✅ Dados limpos! {len(df):,} linhas mantidas')
    return df

# =============================================================
# BLOCO 5 — SALVAR EM PARQUET NO S3
# =============================================================
def salvar_silver(df):
    """
    Salva o DataFrame em Parquet na camada Silver.
    Com 2.2M linhas, o Parquet vai comprimir bastante!
    """
    print('💾 Salvando na camada Silver em formato Parquet...')
    print('   ⏳ Aguarde — arquivo grande...')
    caminho = f'silver/inep/{DATA_HOJE}/aluno.parquet'

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    tamanho_mb = buffer.getbuffer().nbytes / (1024 * 1024)

    s3.put_object(
        Bucket=BUCKET,
        Key=caminho,
        Body=buffer.getvalue()
    )
    print(f'   ✅ Salvo em: s3://{BUCKET}/{caminho}')
    print(f'   📦 Tamanho em Parquet: {tamanho_mb:.1f} MB')
    print(f'   📊 {len(df):,} linhas, {len(df.columns)} colunas')

# =============================================================
# EXECUÇÃO PRINCIPAL
# =============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('SILVER LAYER — ALUNO')
    print(f'Data: {DATA_HOJE}')
    print('=' * 60)

    df = ler_bronze()
    df = padronizar_colunas(df)
    df = tratar_ausentes(df)
    df = limpar_dados(df)
    salvar_silver(df)

    print('=' * 60)
    print('✅ TRANSFORMAÇÃO CONCLUÍDA!')
    print('=' * 60)