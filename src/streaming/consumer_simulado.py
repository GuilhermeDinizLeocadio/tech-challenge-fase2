# =============================================================
# STREAMING — Consumidor de Eventos
#
# RESUMO: Lê os eventos pendentes na fila do S3, processa
# cada um (simulando a atualização do indicador), registra
# um log da operação, e move o evento para a pasta de
# processados. Simula o consumo de uma fila em tempo real.
# =============================================================

import boto3
import json
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

# =============================================================
# BLOCO 1 — LISTAR EVENTOS PENDENTES
# =============================================================
def listar_eventos_pendentes():
    """
    Lista todos os arquivos JSON na pasta de eventos
    pendentes do S3. Analogia: olhar quantos bilhetes
    tem na caixa de entrada.
    """
    print('🔍 Verificando fila de eventos pendentes...')

    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix='streaming/eventos_pendentes/'
    )

    eventos = [
        obj['Key'] for obj in response.get('Contents', [])
        if obj['Key'].endswith('.json')
    ]

    print(f'   ✅ {len(eventos)} eventos encontrados na fila')
    return eventos

# =============================================================
# BLOCO 2 — LER UM EVENTO
# =============================================================
def ler_evento(caminho):
    """
    Lê o conteúdo JSON de um evento específico.
    """
    obj = s3.get_object(Bucket=BUCKET, Key=caminho)
    conteudo = obj['Body'].read().decode('utf-8')
    return json.loads(conteudo)

# =============================================================
# BLOCO 3 — PROCESSAR O EVENTO
# =============================================================
def processar_evento(evento):
    """
    Processa o evento — simula a atualização do indicador.
    Em um sistema real, aqui atualizaríamos um banco de
    dados ou um data warehouse. Na simulação, registramos
    a operação como concluída.
    """
    print(f'\n⚙️  Processando evento: {evento["evento_id"]}')
    print(f'   📍 Município: {evento["nome_municipio"]} ({evento["sigla_uf"]})')
    print(f'   📊 Indicador: {evento["percentual_anterior"]}% → '
          f'{evento["percentual_novo"]}%')

    # Calcula a variação
    variacao = round(
        evento['percentual_novo'] - evento['percentual_anterior'], 2
    )
    tendencia = 'MELHORA' if variacao > 0 else (
        'PIORA' if variacao < 0 else 'ESTAVEL'
    )

    # Atualiza o evento com informações do processamento
    evento['status'] = 'PROCESSADO'
    evento['data_processamento'] = datetime.now().isoformat()
    evento['variacao'] = variacao
    evento['tendencia'] = tendencia

    print(f'   📈 Tendência: {tendencia} ({variacao:+.2f} pontos)')
    print(f'   ✅ Evento processado com sucesso')

    return evento

# =============================================================
# BLOCO 4 — MOVER EVENTO PARA PROCESSADOS
# =============================================================
def mover_para_processados(evento, caminho_original):
    """
    Salva o evento atualizado na pasta de processados
    e remove da pasta de pendentes.
    Analogia: arquivar o bilhete resolvido e tirar da
    caixa de entrada.
    """
    nome_arquivo = evento['evento_id']
    caminho_novo = f'streaming/eventos_processados/{nome_arquivo}.json'

    # Salva na pasta de processados
    s3.put_object(
        Bucket=BUCKET,
        Key=caminho_novo,
        Body=json.dumps(evento, indent=2, ensure_ascii=False),
        ContentType='application/json'
    )

    # Remove da pasta de pendentes
    s3.delete_object(Bucket=BUCKET, Key=caminho_original)

    print(f'   📦 Movido para: {caminho_novo}')

# =============================================================
# BLOCO 5 — REGISTRAR LOG DE EXECUÇÃO
# =============================================================
def registrar_log(eventos_processados):
    """
    Cria um log consolidado da execução do consumer.
    Útil para monitoramento da pipeline.
    """
    log = {
        'execucao_id': f"run_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        'timestamp': datetime.now().isoformat(),
        'total_eventos_processados': len(eventos_processados),
        'eventos': [
            {
                'evento_id': e['evento_id'],
                'municipio': e['nome_municipio'],
                'tendencia': e['tendencia']
            }
            for e in eventos_processados
        ]
    }

    caminho = f'streaming/logs/{log["execucao_id"]}.json'
    s3.put_object(
        Bucket=BUCKET,
        Key=caminho,
        Body=json.dumps(log, indent=2, ensure_ascii=False),
        ContentType='application/json'
    )

    print(f'\n📝 Log de execução salvo: s3://{BUCKET}/{caminho}')

# =============================================================
# EXECUÇÃO PRINCIPAL
# =============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('STREAMING — CONSUMIDOR DE EVENTOS')
    print(f'Timestamp: {datetime.now().isoformat()}')
    print('=' * 60)

    # Lista eventos pendentes
    eventos_pendentes = listar_eventos_pendentes()

    if not eventos_pendentes:
        print('\n⚠️  Nenhum evento pendente na fila!')
    else:
        eventos_processados = []

        for caminho in eventos_pendentes:
            evento = ler_evento(caminho)
            evento_atualizado = processar_evento(evento)
            mover_para_processados(evento_atualizado, caminho)
            eventos_processados.append(evento_atualizado)

        registrar_log(eventos_processados)

        print('\n' + '=' * 60)
        print(f'✅ {len(eventos_processados)} EVENTOS PROCESSADOS!')
        print('=' * 60)