# 🎓 Tech Challenge – Fase 2
## Pipeline Híbrida para Análise da Alfabetização no Brasil

![Python](https://img.shields.io/badge/Python-3.13-blue)
![AWS](https://img.shields.io/badge/AWS-S3-orange)
![Parquet](https://img.shields.io/badge/Format-Parquet-green)
![Status](https://img.shields.io/badge/Status-Concluído-brightgreen)

> Projeto desenvolvido para o Tech Challenge Fase 2 da FIAP PosTech — AI Scientist.
> 📄 Documentação técnica completa em [`docs/documentacao_tecnica.md`](docs/documentacao_tecnica.md)

---

## 📚 Contexto

O **Compromisso Nacional Criança Alfabetizada** estabelece a meta de que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental até **2030**. Este projeto constrói uma pipeline híbrida (Batch + Streaming) que integra dados do INEP e da Base dos Dados para monitorar esse indicador em nível nacional, estadual e municipal.

---

## 🏗️ Arquitetura

Pipeline em **Arquitetura Medalhão** (Bronze → Silver → Gold) na AWS S3, com ingestão híbrida Batch + Streaming.

```
[INEP + Base dos Dados]
        ↓ Batch
   [BRONZE] CSV bruto
        ↓
   [SILVER] Parquet limpo e integrado
        ↓
    [GOLD] Datasets analíticos
        ↓
 [Dashboards / IA]

[Eventos] → Producer → Fila S3 → Consumer → Log
```

📄 Diagrama detalhado, fluxo completo e decisões arquiteturais em [`docs/documentacao_tecnica.md`](docs/documentacao_tecnica.md#arquitetura)

---

## 🛠️ Tecnologias

Python 3.13 · Pandas · PyArrow · Boto3 · AWS S3 · Parquet · Git/GitHub

---

## 📂 Estrutura

```
tech-challenge-fase2/
├── src/
│   ├── ingestion/       ← ingestão Bronze
│   ├── transformation/  ← transformação Silver
│   ├── gold/            ← agregação Gold
│   ├── streaming/       ← producer/consumer
│   └── quality/         ← qualidade e monitoramento
├── docs/                ← documentação técnica completa
├── .env                 ← credenciais (não versionado)
└── requirements.txt
```

---

## ▶️ Como Executar

```bash
git clone https://github.com/GuilhermeDinizLeocadio/tech-challenge-fase2.git
cd tech-challenge-fase2
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Configure o `.env` com suas credenciais AWS e rode a pipeline:

```bash
python src/ingestion/test_connection.py
python src/ingestion/bronze_ingestion.py
python src/quality/bronze_quality.py
python src/transformation/silver_estado.py
python src/transformation/silver_municipio.py
python src/transformation/silver_aluno.py
python src/transformation/silver_metas.py
python src/transformation/silver_integrada.py
python src/gold/gold_ranking_estados.py
python src/gold/gold_metas_vs_resultados.py
python src/gold/gold_painel_nacional.py
python src/streaming/producer_simulado.py
python src/streaming/consumer_simulado.py
python src/quality/pipeline_monitor.py
```

📄 Guia detalhado passo a passo em [`docs/documentacao_tecnica.md`](docs/documentacao_tecnica.md#como-executar)

---

## 📊 Principais Resultados

```
Taxa nacional de alfabetização: 66.15%
Meta 2030: 100% | Distância: 33.85 pontos percentuais
Melhor estado: Goiás (84.09%) | Pior: Roraima (39.24%)
404 municípios em nível crítico (3.25%)
```

📄 Análise completa dos resultados em [`docs/documentacao_tecnica.md`](docs/documentacao_tecnica.md#resultados)

---

## 💰 FinOps

Uso do formato **Parquet** reduziu o armazenamento em **83%** (Bronze: 261MB → Silver: 45MB). Pipeline totalmente serverless, custo estimado de ~$0.007/mês no S3.

📄 Detalhamento de custos e estratégias em [`docs/documentacao_tecnica.md`](docs/documentacao_tecnica.md#finops)

---

## 🤖 Aplicação em IA

A camada Gold está pronta para alimentar modelos preditivos de alfabetização por município, análise de desigualdade educacional e priorização de políticas públicas.

📄 Detalhes das aplicações de IA em [`docs/documentacao_tecnica.md`](docs/documentacao_tecnica.md#aplicação-em-ia)

---

## 👨‍💻 Autor

**Guilherme Diniz Leocadio**
FIAP PosTech — AI Scientist · Tech Challenge Fase 2 · 2026