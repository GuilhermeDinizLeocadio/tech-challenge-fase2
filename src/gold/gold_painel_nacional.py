# =============================================================
# CAMADA GOLD — Painel Nacional de Alfabetização
#
# RESUMO: Consolida todos os dados processados em datasets
# analíticos prontos para dashboards executivos e modelos
# de IA. Responde: "Como está o Brasil na jornada de
# alfabetização até 2030?"
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
DATA_SILVER = '2026-06-15'
DATA_GOLD   = '2026-06-22'
DATA_HOJE   = datetime.now().strftime('%Y-%m-%d')

# =============================================================
# BLOCO 1 — LER DADOS JÁ PROCESSADOS
# =============================================================
def ler_parquet(caminho):
    """Lê qualquer Parquet do S3."""
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    return pd.read_parquet(BytesIO(obj['Body'].read()))

def ler_dados():
    print('📥 Lendo dados da Silver e Gold...')

    # Da Silver
    df_aluno = ler_parquet(f'silver/inep/{DATA_SILVER}/aluno.parquet')
    print(f'   ✅ Alunos: {len(df_aluno):,} linhas')

    df_municipio = ler_parquet(
        f'silver/integrado/{DATA_SILVER}/municipios_com_metas.parquet'
    )
    print(f'   ✅ Municípios: {len(df_municipio):,} linhas')

    # Da Gold
    df_ranking = ler_parquet(f'gold/{DATA_GOLD}/ranking_estados.parquet')
    print(f'   ✅ Ranking estados: {len(df_ranking):,} linhas')

    df_metas = ler_parquet(
        f'gold/{DATA_GOLD}/estados_metas_vs_resultado.parquet'
    )
    print(f'   ✅ Metas vs resultado: {len(df_metas):,} linhas')

    return df_aluno, df_municipio, df_ranking, df_metas

# =============================================================
# BLOCO 2 — PAINEL DE ALUNOS
# =============================================================
def painel_alunos(df):
    """
    Consolida métricas gerais sobre os alunos avaliados.
    Responde: quantas crianças foram avaliadas e
    quantas estão alfabetizadas?
    """
    print('👨‍🎓 Criando painel de alunos...')

    # Filtra só alunos presentes
    df_presentes = df[df['presenca_lingua_portuguesa'] == 1].copy()

    # Calcula métricas gerais
    total_avaliados = len(df_presentes)
    total_alfabetizados = df_presentes['alfabetizado'].sum()
    taxa_geral = (total_alfabetizados / total_avaliados * 100).round(2)

    # Agrupa por estado
    por_estado = df_presentes.groupby('sigla_uf').agg(
        total_alunos=('id_aluno', 'count'),
        total_alfabetizados=('alfabetizado', 'sum'),
    ).reset_index()

    por_estado['taxa_alfabetizacao'] = (
        por_estado['total_alfabetizados'] /
        por_estado['total_alunos'] * 100
    ).round(2)

    por_estado['ranking'] = por_estado['taxa_alfabetizacao'].rank(
        ascending=False, method='min'
    ).astype(int)

    print(f'   ✅ Total avaliados:    {total_avaliados:,}')
    print(f'   ✅ Total alfabetizados: {total_alfabetizados:,}')
    print(f'   ✅ Taxa geral:          {taxa_geral}%')

    return por_estado, taxa_geral

# =============================================================
# BLOCO 3 — PAINEL DE MUNICÍPIOS
# =============================================================
def painel_municipios(df):
    """
    Identifica municípios críticos que precisam
    de intervenção urgente.
    """
    print('🏙️  Criando painel de municípios...')

    # Calcula taxa de alfabetização por município
    df = df.copy()

    # Classifica municípios por nível de desempenho
    if 'perc_alunos_alfabetizados' in df.columns:
        df['nivel_municipio'] = pd.cut(
            df['perc_alunos_alfabetizados'],
            bins=[0, 40, 60, 75, 90, 100],
            labels=['CRITICO', 'BAIXO', 'MEDIO', 'BOM', 'EXCELENTE']
        )

    # Conta municípios por nível
    distribuicao = df['nivel_municipio'].value_counts().reset_index()
    distribuicao.columns = ['nivel', 'qtd_municipios']
    distribuicao['perc_total'] = (
        distribuicao['qtd_municipios'] / len(df) * 100
    ).round(2)

    print(f'   ✅ {len(df):,} municípios analisados')
    print('\n   Distribuição por nível:')
    for _, row in distribuicao.iterrows():
        print(f'   {row["nivel"]}: {row["qtd_municipios"]} '
              f'municípios ({row["perc_total"]}%)')

    return df, distribuicao

# =============================================================
# BLOCO 4 — PAINEL EXECUTIVO BRASIL
# =============================================================
def painel_executivo(taxa_geral, df_ranking, df_municipios_dist):
    """
    Cria o painel executivo consolidado do Brasil.
    É o dataset de mais alto nível — o que um ministro
    da educação veria num dashboard.
    """
    print('📊 Criando painel executivo...')

    # Melhor e pior estado
    melhor = df_ranking.loc[df_ranking['ranking_nacional'].idxmin()]
    pior = df_ranking.loc[df_ranking['ranking_nacional'].idxmax()]

    # Municípios críticos
    municipios_criticos = int(
        df_municipios_dist[
            df_municipios_dist['nivel'] == 'CRITICO'
        ]['qtd_municipios'].sum()
    ) if 'CRITICO' in df_municipios_dist['nivel'].values else 0

    painel = pd.DataFrame({
        'metrica': [
            'Taxa Nacional de Alfabetizacao (%)',
            'Meta Nacional 2030 (%)',
            'Distancia da Meta 2030 (pp)',
            'Melhor Estado',
            'Taxa Melhor Estado (%)',
            'Pior Estado',
            'Taxa Pior Estado (%)',
            'Diferenca Melhor vs Pior (pp)',
            'Municipios em Nivel Critico',
            'Ano de Referencia',
        ],
        'valor': [
            taxa_geral,
            100.0,
            round(100.0 - taxa_geral, 2),
            melhor['sigla_uf'],
            melhor['perc_alunos_alfabetizados'],
            pior['sigla_uf'],
            pior['perc_alunos_alfabetizados'],
            round(
                melhor['perc_alunos_alfabetizados'] -
                pior['perc_alunos_alfabetizados'], 2
            ),
            municipios_criticos,
            2025,
        ]
    })

    # ← ADICIONA ESSA LINHA AQUI
    painel['valor'] = painel['valor'].astype(str)

    print('\n📋 PAINEL EXECUTIVO — BRASIL:')
    for _, row in painel.iterrows():
        print(f'   {row["metrica"]}: {row["valor"]}')

    return painel

# =============================================================
# BLOCO 5 — SALVAR NA GOLD
# =============================================================
def salvar_gold(df, nome):
    print(f'\n💾 Salvando {nome}.parquet na Gold...')
    caminho = f'gold/{DATA_HOJE}/{nome}.parquet'
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=caminho, Body=buffer.getvalue())
    print(f'   ✅ Salvo em: s3://{BUCKET}/{caminho}')
    print(f'   📊 {len(df):,} linhas, {len(df.columns)} colunas')

# =============================================================
# EXECUÇÃO PRINCIPAL
# =============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('GOLD LAYER — PAINEL NACIONAL DE ALFABETIZAÇÃO')
    print(f'Data: {DATA_HOJE}')
    print('=' * 60)

    # Lê todos os dados
    df_aluno, df_municipio, df_ranking, df_metas = ler_dados()

    # Painel de alunos
    df_por_estado, taxa_geral = painel_alunos(df_aluno)
    salvar_gold(df_por_estado, 'painel_alunos_por_estado')

    # Painel de municípios
    df_municipios_enriquecido, df_dist = painel_municipios(df_municipio)
    salvar_gold(df_municipios_enriquecido, 'painel_municipios')
    salvar_gold(df_dist, 'distribuicao_municipios')

    # Painel executivo
    df_executivo = painel_executivo(taxa_geral, df_ranking, df_dist)
    salvar_gold(df_executivo, 'painel_executivo_brasil')

    print('\n' + '=' * 60)
    print('✅ PAINEL NACIONAL CONCLUÍDO!')
    print('=' * 60)