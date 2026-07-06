# =============================================================
# MONITORAMENTO — Pipeline Monitor
#
# RESUMO: Verifica a saúde completa da pipeline de dados.
# Checa se todos os arquivos esperados estão no S3, mede
# volumes processados, verifica o streaming e gera um
# relatório consolidado de status da pipeline.
# =============================================================

import boto3
import os
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
DATA_SILVER = '2026-06-15'
DATA_GOLD   = '2026-06-22'

# =============================================================
# BLOCO 1 — ARQUIVOS ESPERADOS EM CADA CAMADA
# =============================================================

# Define quais arquivos DEVEM existir em cada camada
# Se algum estiver faltando, o monitor vai alertar
ARQUIVOS_ESPERADOS = {
    'Bronze INEP': [
        f'bronze/inep/{DATA_BRONZE}/TS_ALUNO.csv',
        f'bronze/inep/{DATA_BRONZE}/TS_ESTADO.csv',
        f'bronze/inep/{DATA_BRONZE}/TS_ITEM.csv',
        f'bronze/inep/{DATA_BRONZE}/TS_MUNICIPIO.csv',
    ],
    'Bronze Base dos Dados': [
        f'bronze/basedosdados/{DATA_BRONZE}/meta_brasil.csv',
        f'bronze/basedosdados/{DATA_BRONZE}/meta_municipio.csv',
        f'bronze/basedosdados/{DATA_BRONZE}/meta_uf.csv',
        f'bronze/basedosdados/{DATA_BRONZE}/municipio.csv',
        f'bronze/basedosdados/{DATA_BRONZE}/uf.csv',
    ],
    'Silver INEP': [
        f'silver/inep/{DATA_SILVER}/aluno.parquet',
        f'silver/inep/{DATA_SILVER}/estado.parquet',
        f'silver/inep/{DATA_SILVER}/municipio.parquet',
    ],
    'Silver Base dos Dados': [
        f'silver/basedosdados/{DATA_SILVER}/metas.parquet',
    ],
    'Silver Integrado': [
        f'silver/integrado/{DATA_SILVER}/estados_com_metas.parquet',
        f'silver/integrado/{DATA_SILVER}/municipios_com_metas.parquet',
    ],
    'Gold': [
        f'gold/{DATA_GOLD}/ranking_estados.parquet',
        f'gold/{DATA_GOLD}/resumo_regioes.parquet',
        f'gold/{DATA_GOLD}/estados_metas_vs_resultado.parquet',
        f'gold/{DATA_GOLD}/municipios_metas_vs_resultado.parquet',
        f'gold/{DATA_GOLD}/visao_nacional.parquet',
        f'gold/{DATA_GOLD}/painel_alunos_por_estado.parquet',
        f'gold/{DATA_GOLD}/painel_municipios.parquet',
        f'gold/{DATA_GOLD}/distribuicao_municipios.parquet',
        f'gold/{DATA_GOLD}/painel_executivo_brasil.parquet',
    ],
}

# =============================================================
# BLOCO 2 — VERIFICAR ARQUIVOS
# =============================================================
def verificar_arquivos():
    """
    Verifica se todos os arquivos esperados existem no S3.
    Retorna um relatório com status de cada camada.
    """
    print('🔍 Verificando arquivos em cada camada...\n')

    resultados = {}
    total_ok = 0
    total_falha = 0

    for camada, arquivos in ARQUIVOS_ESPERADOS.items():
        print(f'📁 {camada}:')
        camada_ok = 0
        camada_falha = 0
        detalhes = []

        for arquivo in arquivos:
            try:
                obj = s3.head_object(Bucket=BUCKET, Key=arquivo)
                tamanho_mb = obj['ContentLength'] / (1024 * 1024)
                print(f'   ✅ {arquivo.split("/")[-1]} ({tamanho_mb:.2f} MB)')
                camada_ok += 1
                total_ok += 1
                detalhes.append({
                    'arquivo': arquivo,
                    'status': 'OK',
                    'tamanho_mb': round(tamanho_mb, 2)
                })
            except Exception:
                print(f'   ❌ FALTANDO: {arquivo.split("/")[-1]}')
                camada_falha += 1
                total_falha += 1
                detalhes.append({
                    'arquivo': arquivo,
                    'status': 'FALTANDO',
                    'tamanho_mb': 0
                })

        status_camada = '✅ OK' if camada_falha == 0 else '❌ INCOMPLETA'
        print(f'   → Status: {status_camada} ({camada_ok}/{len(arquivos)})\n')

        resultados[camada] = {
            'status': status_camada,
            'ok': camada_ok,
            'falha': camada_falha,
            'detalhes': detalhes
        }

    return resultados, total_ok, total_falha

# =============================================================
# BLOCO 3 — MEDIR VOLUMES
# =============================================================
def medir_volumes():
    """
    Calcula o tamanho total de cada camada no S3.
    Útil para FinOps — saber quanto espaço estamos usando.
    """
    print('📏 Medindo volumes por camada...')

    prefixos = {
        'Bronze': 'bronze/',
        'Silver': 'silver/',
        'Gold':   'gold/',
        'Streaming': 'streaming/'
    }

    volumes = {}
    for nome, prefixo in prefixos.items():
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefixo)
        objetos = response.get('Contents', [])
        tamanho_total = sum(obj['Size'] for obj in objetos)
        tamanho_mb = tamanho_total / (1024 * 1024)
        qtd_arquivos = len(objetos)
        volumes[nome] = {
            'tamanho_mb': round(tamanho_mb, 2),
            'qtd_arquivos': qtd_arquivos
        }
        print(f'   {nome}: {tamanho_mb:.2f} MB ({qtd_arquivos} arquivos)')

    total_mb = sum(v['tamanho_mb'] for v in volumes.values())
    print(f'\n   💾 Total no S3: {total_mb:.2f} MB')
    return volumes, total_mb

# =============================================================
# BLOCO 4 — VERIFICAR STREAMING
# =============================================================
def verificar_streaming():
    """
    Verifica o status do Streaming — quantos eventos
    foram processados e se há pendentes.
    """
    print('\n📡 Verificando Streaming...')

    # Eventos pendentes
    pendentes = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix='streaming/eventos_pendentes/'
    )
    qtd_pendentes = len([
        o for o in pendentes.get('Contents', [])
        if o['Key'].endswith('.json')
    ])

    # Eventos processados
    processados = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix='streaming/eventos_processados/'
    )
    qtd_processados = len([
        o for o in processados.get('Contents', [])
        if o['Key'].endswith('.json')
    ])

    # Logs de execução
    logs = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix='streaming/logs/'
    )
    qtd_logs = len([
        o for o in logs.get('Contents', [])
        if o['Key'].endswith('.json')
    ])

    print(f'   📬 Eventos pendentes:   {qtd_pendentes}')
    print(f'   ✅ Eventos processados: {qtd_processados}')
    print(f'   📝 Logs de execução:    {qtd_logs}')

    status = '✅ OK' if qtd_pendentes == 0 else '⚠️ HÁ EVENTOS PENDENTES'
    print(f'   → Status: {status}')

    return {
        'pendentes': qtd_pendentes,
        'processados': qtd_processados,
        'logs': qtd_logs,
        'status': status
    }

# =============================================================
# BLOCO 5 — RELATÓRIO FINAL
# =============================================================
def gerar_relatorio(resultados, total_ok, total_falha,
                    volumes, total_mb, streaming):
    """
    Imprime o relatório consolidado de saúde da pipeline.
    """
    print('\n' + '=' * 60)
    print('RELATÓRIO DE SAÚDE DA PIPELINE')
    print(f'Gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    print('=' * 60)

    # Status geral
    status_geral = '✅ SAUDÁVEL' if total_falha == 0 else '❌ ATENÇÃO NECESSÁRIA'
    print(f'\n🏥 Status Geral: {status_geral}')
    print(f'   Arquivos OK:      {total_ok}')
    print(f'   Arquivos faltando: {total_falha}')

    # Volumes
    print(f'\n💾 Uso de Armazenamento:')
    for nome, dados in volumes.items():
        print(f'   {nome}: {dados["tamanho_mb"]} MB '
              f'({dados["qtd_arquivos"]} arquivos)')
    print(f'   Total: {total_mb:.2f} MB')

    # Streaming
    print(f'\n📡 Streaming:')
    print(f'   Eventos processados: {streaming["processados"]}')
    print(f'   Eventos pendentes:   {streaming["pendentes"]}')
    print(f'   Status: {streaming["status"]}')

    # Alerta final
    if total_falha > 0:
        print('\n⚠️  ALERTAS:')
        for camada, dados in resultados.items():
            if dados['falha'] > 0:
                print(f'   → {camada}: {dados["falha"]} arquivo(s) faltando!')
    else:
        print('\n✅ Todos os arquivos estão presentes e a pipeline está saudável!')

    print('=' * 60)

# =============================================================
# EXECUÇÃO PRINCIPAL
# =============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('MONITORAMENTO DA PIPELINE')
    print(f'Data: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    print('=' * 60 + '\n')

    resultados, total_ok, total_falha = verificar_arquivos()
    volumes, total_mb = medir_volumes()
    streaming = verificar_streaming()
    gerar_relatorio(
        resultados, total_ok, total_falha,
        volumes, total_mb, streaming
    )