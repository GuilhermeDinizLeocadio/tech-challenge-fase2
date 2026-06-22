# =============================================================
# CAMADA GOLD — Ranking de Estados por Alfabetização
#
# RESUMO: Lê os dados integrados da Silver (estados com metas),
# filtra a rede estadual (tipo 2), calcula rankings por estado,
# classifica por desempenho e salva na camada Gold.
# =============================================================

import boto3
import pandas as pd
import os
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

BUCKET = os.getenv('AWS_BUCKET_NAME')
DATA_SILVER = '2026-06-15'
DATA_HOJE = datetime.now().strftime('%Y-%m-%d')

def ler_estados_silver():
    print('📥 Lendo estados_com_metas.parquet da Silver...')
    caminho = f'silver/integrado/{DATA_SILVER}/estados_com_metas.parquet'
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    buffer = BytesIO(obj['Body'].read())
    df = pd.read_parquet(buffer, engine='pyarrow')
    print(f'   ✅ {len(df):,} linhas, {len(df.columns)} colunas')
    return df

def calcular_rankings(df):
    print('🏆 Calculando rankings...')

    # Filtra rede estadual (tipo 2) para comparação justa
    df_publico = df[df['ID_TIPO_REDE'] == 2].copy()
    print(f'   ℹ️  Filtrando rede estadual (tipo 2): {len(df_publico)} estados')

    # Ranking por taxa de alfabetização — rank 1 = melhor estado
    df_publico['ranking_nacional'] = df_publico['perc_alunos_alfabetizados'].rank(
        ascending=False,
        method='min'
    ).astype(int)

    # Diferença entre resultado e meta 2024
    if 'meta_2024' in df_publico.columns:
        df_publico['diferenca_meta_2024'] = (
            df_publico['perc_alunos_alfabetizados'] -
            df_publico['meta_2024']
        ).round(2)

        df_publico['classificacao'] = df_publico['diferenca_meta_2024'].apply(
            lambda x: 'ACIMA_DA_META' if x > 0
            else ('NA_META' if x == 0 else 'ABAIXO_DA_META')
        )

    # Mapeia região de cada estado
    regioes = {
        'AC': 'Norte', 'AM': 'Norte', 'AP': 'Norte',
        'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste',
        'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste',
        'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
        'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste',
        'MS': 'Centro-Oeste', 'MT': 'Centro-Oeste',
        'ES': 'Sudeste', 'MG': 'Sudeste',
        'RJ': 'Sudeste', 'SP': 'Sudeste',
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    }
    df_publico['regiao'] = df_publico['sigla_uf'].map(regioes)

    print(f'   ✅ Rankings calculados para {len(df_publico)} estados')
    return df_publico

def resumo_por_regiao(df):
    print('🗺️  Criando resumo por região...')

    resumo = df.groupby('regiao').agg(
        media_alfabetizacao=('perc_alunos_alfabetizados', 'mean'),
        media_lingua_portuguesa=('media_lingua_portuguesa', 'mean'),
        qtd_estados=('sigla_uf', 'nunique'),
        estados_acima_meta=('atingiu_meta_2024',
            lambda x: x.sum())
    ).reset_index()

    resumo['media_alfabetizacao'] = resumo['media_alfabetizacao'].round(2)
    resumo['media_lingua_portuguesa'] = resumo['media_lingua_portuguesa'].round(2)
    resumo['perc_estados_acima_meta'] = (
        resumo['estados_acima_meta'] / resumo['qtd_estados'] * 100
    ).round(2)

    print(f'   ✅ Resumo criado para {len(resumo)} regiões')
    return resumo

def salvar_gold(df, nome):
    print(f'💾 Salvando {nome}.parquet na Gold...')
    caminho = f'gold/{DATA_HOJE}/{nome}.parquet'
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=caminho, Body=buffer.getvalue())
    print(f'   ✅ Salvo em: s3://{BUCKET}/{caminho}')
    print(f'   📊 {len(df):,} linhas, {len(df.columns)} colunas')

if __name__ == '__main__':
    print('=' * 60)
    print('GOLD LAYER — RANKING DE ESTADOS')
    print(f'Data: {DATA_HOJE}')
    print('=' * 60)

    df = ler_estados_silver()
    df_ranking = calcular_rankings(df)
    df_regiao = resumo_por_regiao(df_ranking)

    salvar_gold(df_ranking, 'ranking_estados')
    salvar_gold(df_regiao, 'resumo_regioes')

    print('\n📋 TOP 5 ESTADOS MAIS ALFABETIZADOS:')
    top5 = df_ranking.nsmallest(5, 'ranking_nacional')[
        ['ranking_nacional', 'sigla_uf', 'regiao',
         'perc_alunos_alfabetizados', 'classificacao']
    ]
    print(top5.to_string(index=False))

    print('\n📋 5 ESTADOS QUE MAIS PRECISAM DE ATENÇÃO:')
    bottom5 = df_ranking.nlargest(5, 'ranking_nacional')[
        ['ranking_nacional', 'sigla_uf', 'regiao',
         'perc_alunos_alfabetizados', 'classificacao']
    ]
    print(bottom5.to_string(index=False))

    print('\n' + '=' * 60)
    print('✅ GOLD LAYER CONCLUÍDA!')
    print('=' * 60)