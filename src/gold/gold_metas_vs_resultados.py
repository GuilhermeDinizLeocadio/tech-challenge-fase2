# =============================================================
# CAMADA GOLD — Metas vs Resultado
#
# RESUMO: Lê os dados integrados da Silver e cria um dataset
# analítico comparando os resultados reais de alfabetização
# com as metas estabelecidas para cada ano (2024-2030).
# Identifica estados no caminho certo e os que precisam
# de atenção para atingir a meta nacional.
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

def ler_dados_silver():
    print('📥 Lendo dados da Silver...')

    obj = s3.get_object(
        Bucket=BUCKET,
        Key=f'silver/integrado/{DATA_SILVER}/estados_com_metas.parquet'
    )
    df_estados = pd.read_parquet(BytesIO(obj['Body'].read()))

    # Filtra rede estadual (tipo 2) para comparação justa
    df_estados = df_estados[df_estados['ID_TIPO_REDE'] == 2].copy()
    print(f'   ✅ Estados: {len(df_estados):,} linhas (rede estadual)')

    obj = s3.get_object(
        Bucket=BUCKET,
        Key=f'silver/integrado/{DATA_SILVER}/municipios_com_metas.parquet'
    )
    df_municipios = pd.read_parquet(BytesIO(obj['Body'].read()))
    print(f'   ✅ Municípios: {len(df_municipios):,} linhas')

    return df_estados, df_municipios

def analisar_metas(df):
    print('📊 Analisando metas...')
    df = df.copy()

    # Distância para cada meta anual
    for ano in [2024, 2025, 2026, 2027, 2028, 2029, 2030]:
        col_meta = f'meta_{ano}'
        col_dist = f'distancia_meta_{ano}'
        if col_meta in df.columns:
            df[col_dist] = (
                df['perc_alunos_alfabetizados'] - df[col_meta]
            ).round(2)

    # Classifica urgência
    if 'distancia_meta_2024' in df.columns:
        df['urgencia'] = df['distancia_meta_2024'].apply(
            lambda x: 'CRITICO' if x < -20
            else ('ATENCAO' if x < -10
            else ('PROXIMO' if x < 0
            else 'ATINGIDO'))
        )

    # Esforço necessário até 2030
    if 'meta_2030' in df.columns:
        df['esforco_ate_2030'] = (
            df['meta_2030'] - df['perc_alunos_alfabetizados']
        ).round(2)
        df['esforco_por_ano'] = (
            df['esforco_ate_2030'] / 6
        ).round(2)

    print(f'   ✅ Análise concluída para {len(df):,} registros')
    return df

def criar_visao_nacional(df_estados):
    print('🇧🇷 Criando visão nacional...')

    # Pega ano mais recente
    ano_mais_recente = df_estados['ano_avaliacao'].max()
    df_recente = df_estados[
        df_estados['ano_avaliacao'] == ano_mais_recente
    ].copy()
    print(f'   ℹ️  {len(df_recente)} estados analisados')

    media_nacional = df_recente['perc_alunos_alfabetizados'].mean().round(2)

    resumo = pd.DataFrame({
        'indicador': [
            'Media Nacional Alfabetizacao (%)',
            'Estados em situacao CRITICA (>20pp abaixo da meta)',
            'Estados em ATENCAO (10-20pp abaixo da meta)',
            'Estados PROXIMOS da meta (<10pp abaixo)',
            'Estados que ATINGIRAM a meta',
            'Ano de referencia'
        ],
        'valor': [
            media_nacional,
            int((df_recente['urgencia'] == 'CRITICO').sum()),
            int((df_recente['urgencia'] == 'ATENCAO').sum()),
            int((df_recente['urgencia'] == 'PROXIMO').sum()),
            int((df_recente['urgencia'] == 'ATINGIDO').sum()),
            ano_mais_recente
        ]
    })

    print('\n📊 SITUAÇÃO ATUAL DO BRASIL:')
    for _, row in resumo.iterrows():
        print(f'   {row["indicador"]}: {row["valor"]}')

    return resumo

def salvar_gold(df, nome):
    print(f'\n💾 Salvando {nome}.parquet na Gold...')
    caminho = f'gold/{DATA_HOJE}/{nome}.parquet'
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=caminho, Body=buffer.getvalue())
    print(f'   ✅ Salvo em: s3://{BUCKET}/{caminho}')
    print(f'   📊 {len(df):,} linhas, {len(df.columns)} colunas')

if __name__ == '__main__':
    print('=' * 60)
    print('GOLD LAYER — METAS VS RESULTADO')
    print(f'Data: {DATA_HOJE}')
    print('=' * 60)

    df_estados, df_municipios = ler_dados_silver()

    df_estados_analisado = analisar_metas(df_estados)
    salvar_gold(df_estados_analisado, 'estados_metas_vs_resultado')

    df_municipios_analisado = analisar_metas(df_municipios)
    salvar_gold(df_municipios_analisado, 'municipios_metas_vs_resultado')

    df_nacional = criar_visao_nacional(df_estados_analisado)
    salvar_gold(df_nacional, 'visao_nacional')

    print('\n' + '=' * 60)
    print('✅ GOLD LAYER CONCLUÍDA!')
    print('=' * 60)