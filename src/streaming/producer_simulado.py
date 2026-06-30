# =============================================================
# STREAMING — Produtor de Eventos Simulados
#
# RESUMO: Simula a chegada de uma nova atualização de
# indicador de alfabetização para um município, como se
# fosse enviada pelo INEP em tempo real. Gera um evento
# JSON e salva na pasta de eventos pendentes no S3.
# =============================================================

import boto3
import pandas as pd
import json
import os
import random
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

# =============================================================
# BLOCO 1 — BUSCAR MUNICÍPIOS REAIS PARA SIMULAR
# =============================================================
def buscar_municipios_existentes():
    """
    Busca municípios reais da Silver para usar como base
    da simulação. Assim os eventos parecem realistas.
    """
    print('📥 Buscando municípios para simular eventos...')
    caminho = f'silver/integrado/{DATA_SILVER}/municipios_com_metas.parquet'
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    df = pd.read_parquet(BytesIO(obj['Body'].read()))

    # Seleciona só as colunas que precisamos
    colunas = ['codigo_municipio', 'nome_municipio', 'sigla_uf',
               'perc_alunos_alfabetizados']
    df = df[colunas].dropna()

    print(f'   ✅ {len(df):,} municípios disponíveis para simulação')
    return df

# =============================================================
# BLOCO 2 — GERAR UM EVENTO SIMULADO
# =============================================================
def gerar_evento(df_municipios):
    """
    Gera um evento simulando uma nova medição de
    desempenho para um município aleatório.
    Analogia: o INEP "liga" avisando um resultado novo.
    """
    print('📡 Gerando evento simulado...')

    # Escolhe um município aleatório como base
    municipio = df_municipios.sample(1).iloc[0]

    # Simula uma pequena variação no indicador
    # (como se fosse uma nova medição/reavaliação)
    variacao = random.uniform(-3.0, 5.0)
    novo_percentual = round(
        max(0, min(100, municipio['perc_alunos_alfabetizados'] + variacao)),
        2
    )

    evento = {
        'evento_id': f"evt_{datetime.now().strftime('%Y%m%d%H%M%S')}_{random.randint(1000,9999)}",
        'tipo_evento': 'ATUALIZACAO_INDICADOR',
        'timestamp': datetime.now().isoformat(),
        'codigo_municipio': str(municipio['codigo_municipio']),
        'nome_municipio': municipio['nome_municipio'],
        'sigla_uf': municipio['sigla_uf'],
        'percentual_anterior': float(municipio['perc_alunos_alfabetizados']),
        'percentual_novo': novo_percentual,
        'fonte': 'SIMULACAO_INEP_REALTIME',
        'status': 'PENDENTE'
    }

    print(f'   ✅ Evento gerado: {evento["evento_id"]}')
    print(f'   📍 Município: {evento["nome_municipio"]} ({evento["sigla_uf"]})')
    print(f'   📊 {evento["percentual_anterior"]}% → {evento["percentual_novo"]}%')

    return evento

# =============================================================
# BLOCO 3 — SALVAR EVENTO NO S3
# =============================================================
def salvar_evento(evento):
    """
    Salva o evento como JSON na pasta de eventos pendentes.
    Pensa como deixar um bilhete na caixa de entrada
    esperando alguém processar.
    """
    print('💾 Salvando evento na fila do S3...')

    caminho = f'streaming/eventos_pendentes/{evento["evento_id"]}.json'

    s3.put_object(
        Bucket=BUCKET,
        Key=caminho,
        Body=json.dumps(evento, indent=2, ensure_ascii=False),
        ContentType='application/json'
    )

    print(f'   ✅ Salvo em: s3://{BUCKET}/{caminho}')

# =============================================================
# EXECUÇÃO PRINCIPAL
# =============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('STREAMING — PRODUTOR DE EVENTOS')
    print(f'Timestamp: {datetime.now().isoformat()}')
    print('=' * 60)

    df_municipios = buscar_municipios_existentes()
    evento = gerar_evento(df_municipios)
    salvar_evento(evento)

    print('=' * 60)
    print('✅ EVENTO PUBLICADO NA FILA!')
    print('=' * 60)