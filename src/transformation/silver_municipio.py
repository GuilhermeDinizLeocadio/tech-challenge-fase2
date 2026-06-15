# =============================================================
# CAMADA SILVER — Transformação dos dados de Município
#
# RESUMO: Lê TS_MUNICIPIO.csv (INEP) e municipio.csv (Base dos
# Dados) da camada Bronze, limpa e padroniza cada um, cruza as
# duas fontes pelo código do município e salva em Parquet na
# camada Silver.
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
# BLOCO 1 — LER ARQUIVOS DA CAMADA BRONZE
# =============================================================
def ler_bronze_inep():
    """
    Lê o TS_MUNICIPIO.csv do INEP da camada Bronze.
    Contém resultados da avaliação por município.
    """
    print('📥 Lendo TS_MUNICIPIO.csv (INEP)...')
    caminho = f'bronze/inep/{DATA_BRONZE}/TS_MUNICIPIO.csv'
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    conteudo = obj['Body'].read().decode('latin-1')
    df = pd.read_csv(StringIO(conteudo), sep=';', low_memory=False)
    print(f'   ✅ Lido! {len(df):,} linhas, {len(df.columns)} colunas')
    return df

def ler_bronze_basedosdados():
    """
    Lê o municipio.csv da Base dos Dados da camada Bronze.
    Contém dados complementares dos municípios.
    """
    print('📥 Lendo municipio.csv (Base dos Dados)...')
    caminho = f'bronze/basedosdados/{DATA_BRONZE}/municipio.csv'
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    conteudo = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(conteudo), sep=',', low_memory=False)
    print(f'   ✅ Lido! {len(df):,} linhas, {len(df.columns)} colunas')
    return df

# =============================================================
# BLOCO 2 — PADRONIZAR COLUNAS DO INEP
# =============================================================
def padronizar_inep(df):
    """
    Renomeia as colunas do TS_MUNICIPIO para nomes legíveis.
    """
    print('🏷️  Padronizando colunas do INEP...')
    mapeamento = {
        'NU_ANO_AVALIACAO':      'ano_avaliacao',
        'CO_UF':                 'codigo_uf',
        'SG_UF':                 'sigla_uf',
        'CO_MUNICIPIO':          'codigo_municipio',
        'NO_MUNICIPIO':          'nome_municipio',
        'TP_DEPENDENCIA':        'tipo_dependencia',
        'QT_ALUNO_MATRICULADO':  'qtd_alunos_matriculados',
        'QT_ALUNO_AVALIADO':     'qtd_alunos_avaliados',
        'QT_ALUNO_ALFABETIZADO': 'qtd_alunos_alfabetizados',
        'PC_ALUNO_ALFABETIZADO': 'perc_alunos_alfabetizados',
        'VL_MEDIA_LP':           'media_lingua_portuguesa',
        'PC_NIVEL_0':            'perc_nivel_0',
        'PC_NIVEL_1':            'perc_nivel_1',
        'PC_NIVEL_2':            'perc_nivel_2',
        'PC_NIVEL_3':            'perc_nivel_3',
    }
    colunas_existentes = {k: v for k, v in mapeamento.items() if k in df.columns}
    df = df.rename(columns=colunas_existentes)
    print(f'   ✅ {len(colunas_existentes)} colunas renomeadas')
    return df

# =============================================================
# BLOCO 3 — PADRONIZAR COLUNAS DA BASE DOS DADOS
# =============================================================
def padronizar_basedosdados(df):
    """
    Seleciona e padroniza as colunas relevantes do municipio.csv.
    """
    print('🏷️  Padronizando colunas da Base dos Dados...')

    # Seleciona só as colunas que nos interessam
    colunas_interesse = [
        'id_municipio',
        'ano',
        'sigla_uf',
        'taxa_alfabetizacao',
        'media_portugues',
    ]

    # Pega só as colunas que existem
    colunas_existentes = [c for c in colunas_interesse if c in df.columns]
    df = df[colunas_existentes].copy()

    # Renomeia para evitar conflito no merge
    df = df.rename(columns={
        'id_municipio': 'codigo_municipio',
        'ano':          'ano_avaliacao',
        'taxa_alfabetizacao': 'taxa_alfabetizacao_bd',
        'media_portugues':    'media_portugues_bd',
    })

    print(f'   ✅ {len(df.columns)} colunas selecionadas')
    return df

# =============================================================
# BLOCO 4 — LIMPAR OS DADOS
# =============================================================
def limpar_dados(df):
    """
    Limpeza geral — remove espaços, padroniza tipos.
    """
    print('🧹 Limpando os dados...')

    # Remove espaços extras em texto
    colunas_texto = df.select_dtypes(include='object').columns
    for col in colunas_texto:
        df[col] = df[col].str.strip()

    # Padroniza tipos numéricos
    colunas_num = [c for c in df.columns if any(
        x in c for x in ['perc_', 'media_', 'qtd_', 'taxa_']
    )]
    for col in colunas_num:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Garante que codigo_municipio é string padronizada
    if 'codigo_municipio' in df.columns:
        df['codigo_municipio'] = df['codigo_municipio'].astype(str).str.strip()

    print(f'   ✅ Dados limpos! {len(df):,} linhas mantidas')
    return df

# =============================================================
# BLOCO 5 — CRUZAR AS DUAS FONTES
# =============================================================
def cruzar_fontes(df_inep, df_bd):
    """
    Cruza os dados do INEP com os da Base dos Dados
    usando o código do município como chave.
    Analogia: juntar duas listas de contatos pelo CPF.
    """
    print('🔗 Cruzando INEP com Base dos Dados...')

    # Garante que a chave de join é string nos dois DataFrames
    df_inep['codigo_municipio'] = df_inep['codigo_municipio'].astype(str).str.strip()
    df_bd['codigo_municipio'] = df_bd['codigo_municipio'].astype(str).str.strip()

    # Left join — mantém todos os municípios do INEP
    # mesmo que não tenham correspondência na Base dos Dados
    df_final = df_inep.merge(
        df_bd,
        on=['codigo_municipio', 'ano_avaliacao'],
        how='left'
    )

    # Adiciona coluna de controle
    df_final['data_processamento'] = DATA_HOJE

    print(f'   ✅ Cruzamento concluído!')
    print(f'   📊 {len(df_final):,} linhas, {len(df_final.columns)} colunas')
    return df_final

# =============================================================
# BLOCO 6 — SALVAR EM PARQUET NO S3
# =============================================================
def salvar_silver(df):
    """
    Salva o DataFrame final em Parquet na camada Silver.
    """
    print('💾 Salvando na camada Silver em formato Parquet...')
    caminho = f'silver/inep/{DATA_HOJE}/municipio.parquet'

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
    print('SILVER LAYER — MUNICÍPIO')
    print(f'Data: {DATA_HOJE}')
    print('=' * 60)

    # Lê as duas fontes
    df_inep = ler_bronze_inep()
    df_bd = ler_bronze_basedosdados()

    # Padroniza cada uma
    df_inep = padronizar_inep(df_inep)
    df_bd = padronizar_basedosdados(df_bd)

    # Limpa as duas
    df_inep = limpar_dados(df_inep)
    df_bd = limpar_dados(df_bd)

    # Cruza as fontes
    df_final = cruzar_fontes(df_inep, df_bd)

    # Salva na Silver
    salvar_silver(df_final)

    print('=' * 60)
    print('✅ TRANSFORMAÇÃO CONCLUÍDA!')
    print('=' * 60)