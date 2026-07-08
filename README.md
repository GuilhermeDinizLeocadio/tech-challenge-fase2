# 🎓 Tech Challenge – Fase 2
## Pipeline Híbrida para Análise da Alfabetização no Brasil


> Projeto desenvolvido para o Tech Challenge Fase 2 da FIAP PosTech — AI Scientist.  
> 📄 Documentação técnica completa em [`docs/documentacao_tecnica.pdf`](docs/documentacao_tecnica.pdf)

---

## 📚 Contexto do Problema

O **Compromisso Nacional Criança Alfabetizada**, instituído pelo Decreto nº 11.556 de 2023, estabelece a meta de que todas as crianças brasileiras estejam alfabetizadas até o final do **2º ano do ensino fundamental até 2030**. Para monitorar esse compromisso, o INEP definiu o ponto de corte de **743 pontos na escala do SAEB** como nível mínimo de alfabetização.

Em 2025, o Brasil alfabetizou **66,15%** das crianças avaliadas — uma distância de 33,85 pontos percentuais da meta. Compreender os fatores que influenciam esse indicador exige integrar múltiplas fontes heterogêneas: microdados educacionais, metas nacionais e estaduais, dados territoriais e indicadores de desempenho.

Sem uma pipeline integrada, é impossível responder perguntas estratégicas como:
- Quais estados estão no caminho certo para atingir a meta de 2030?
- Quais municípios precisam de intervenção urgente?
- Qual é a magnitude da desigualdade regional na alfabetização?

---

## 🏗️ Arquitetura da Solução

Pipeline em **Arquitetura Medalhão** (Bronze → Silver → Gold) na AWS S3, com ingestão híbrida Batch + Streaming.

```
┌─────────────────────────────────────────────┐
│             FONTES DE DADOS                 │
│   INEP AEEB 2025        Base dos Dados      │
│  (2,2M registros)   (metas e territórios)   │
└──────────────┬──────────────────────────────┘
               │ BATCH
               ▼
┌─────────────────────────────────────────────┐
│  BRONZE — CSV bruto, sem transformações     │
│  Particionado por data de ingestão          │
└──────────────┬──────────────────────────────┘
               │ Python + Pandas
               ▼
┌─────────────────────────────────────────────┐
│  SILVER — Parquet limpo e integrado         │
│  Limpeza, padronização, join das fontes     │
│  83% menor que o Bronze                     │
└──────────────┬──────────────────────────────┘
               │ Agregação e enriquecimento
               ▼
┌─────────────────────────────────────────────┐
│  GOLD — Datasets analíticos prontos         │
│  Ranking, metas vs resultados, painel       │
└──────────────┬──────────────────────────────┘
               ▼
        [Dashboards / IA]

┌─────────────────────────────────────────────┐
│  STREAMING                                  │
│  Producer → eventos_pendentes/ (S3)         │
│  Consumer → eventos_processados/ + logs/    │
└─────────────────────────────────────────────┘
```

📄 Diagrama detalhado e fluxo completo em [`docs/documentacao_tecnica.pdf`](docs/documentacao_tecnica.pdf)

---

## 📦 Fontes de Dados

| Fonte | Entidades | Descrição |
|---|---|---|
| **INEP AEEB 2025** | TS_ALUNO, TS_ESTADO, TS_MUNICIPIO, TS_ITEM | Microdados oficiais das avaliações estaduais — 2,2M registros de 27 estados |
| **Base dos Dados** | meta_brasil, meta_uf, meta_municipio, municipio, uf | Metas nacionais e estaduais de alfabetização (2024–2030) |

> **Decisão importante:** A tabela de alunos da Base dos Dados foi descontinuada durante o desenvolvimento. Substituímos pelos Microdados Oficiais INEP AEEB 2025, que contêm dados mais detalhados e atualizados. Essa substituição demonstrou maturidade técnica — encontramos um obstáculo e identificamos uma solução melhor.

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Justificativa |
|---|---|
| **Python 3.13** | Linguagem principal — ecossistema rico para dados e engenharia |
| **Pandas 2.x** | Manipulação e transformação de DataFrames — padrão da indústria |
| **PyArrow** | Leitura/escrita de Parquet com alta performance |
| **Boto3** | SDK oficial AWS para acesso programático ao S3 |
| **AWS S3** | Data Lake escalável, serverless e de baixo custo |
| **Parquet** | Formato colunar — 83% menor que CSV no projeto |
| **Git + GitHub** | Versionamento, branches e Pull Requests |
| **Python-dotenv** | Gerenciamento seguro de credenciais |

---

## ⚖️ Decisões Arquiteturais

### Batch vs Streaming

| Critério | Batch | Streaming |
|---|---|---|
| Volume | Alto (2,2M registros) | Baixo (eventos individuais) |
| Frequência | Diária/semanal | Tempo real |
| Latência | Minutos | Segundos |
| Custo | Menor | Maior |
| Uso no projeto | Dados históricos INEP | Atualizações de indicadores |

**Decisão:** Arquitetura híbrida — Batch para dados históricos volumosos, Streaming para simular atualizações em tempo real de indicadores municipais.

### Data Lake vs Data Warehouse

| Critério | Data Lake (escolhido) | Data Warehouse |
|---|---|---|
| Flexibilidade | Alta — qualquer formato | Baixa — esquema rígido |
| Custo | Baixo — S3 | Alto — Redshift, BigQuery |
| Escalabilidade | Ilimitada | Limitada pelo cluster |

**Decisão:** Data Lake no AWS S3. A camada Gold funciona como um Data Mart — datasets pré-agregados e prontos para consumo.

### Custo vs Performance

Optamos por **Parquet** em vez de CSV nas camadas Silver e Gold: redução de 83% no armazenamento (261 MB → 45 MB) com ganho de performance na leitura colunar. Pipeline totalmente serverless — sem EC2 rodando 24/7, custo zero quando não está em execução.

📄 Análise completa de trade-offs em [`docs/documentacao_tecnica.pdf`](docs/documentacao_tecnica.pdf)

---

## 📂 Estrutura do Repositório

```
tech-challenge-fase2/
├── src/
│   ├── ingestion/
│   │   ├── bronze_ingestion.py   ← coleta e envia CSVs para o S3
│   │   └── test_connection.py    ← valida conexão com AWS
│   ├── transformation/
│   │   ├── silver_aluno.py       ← limpa e padroniza microdados INEP
│   │   ├── silver_estado.py
│   │   ├── silver_municipio.py
│   │   ├── silver_metas.py
│   │   └── silver_integrada.py   ← integra INEP + Base dos Dados
│   ├── gold/
│   │   ├── gold_ranking_estados.py
│   │   ├── gold_metas_vs_resultados.py
│   │   └── gold_painel_nacional.py
│   ├── streaming/
│   │   ├── producer_simulado.py  ← gera eventos municipais
│   │   └── consumer_simulado.py  ← processa e registra logs no S3
│   └── quality/
│       ├── bronze_quality.py     ← validação dos 9 arquivos Bronze
│       └── pipeline_monitor.py   ← monitoramento completo da pipeline
├── docs/
│   └── documentacao_tecnica.pdf  ← documentação técnica completa
├── .env.example                  ← template de credenciais
├── requirements.txt
└── README.md
```

---

## 🔍 Qualidade de Dados

O script `bronze_quality.py` executa validações em todos os 9 arquivos da camada Bronze:

- **Duplicatas** — contagem de linhas idênticas
- **Valores ausentes** — detecção e contagem por coluna
- **Tipos de dados** — verificação de consistência
- **Volume** — contagem de linhas e colunas

Resultado: 3 arquivos OK, 6 com alertas documentados nas fontes originais (nenhum crítico para as análises).

---

## 📡 Monitoramento da Pipeline

O script `pipeline_monitor.py` verifica a saúde de todas as camadas em uma única execução:

- Presença dos **24 arquivos esperados** no S3
- Volume em MB por camada (Bronze / Silver / Gold / Streaming)
- Status do Streaming (eventos pendentes vs processados)
- Alertas automáticos para arquivos faltantes

**Último resultado:** Status SAUDÁVEL — 24/24 arquivos OK, 3 eventos processados, 0 pendentes.

---

## ▶️ Como Executar

```bash
git clone https://github.com/GuilhermeDinizLeocadio/tech-challenge-fase2.git
cd tech-challenge-fase2
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Configure o `.env` com suas credenciais AWS (use `.env.example` como base) e rode a pipeline na ordem:

```bash
# 1. Validar conexão
python src/ingestion/test_connection.py

# 2. Camada Bronze
python src/ingestion/bronze_ingestion.py
python src/quality/bronze_quality.py

# 3. Camada Silver
python src/transformation/silver_estado.py
python src/transformation/silver_municipio.py
python src/transformation/silver_aluno.py
python src/transformation/silver_metas.py
python src/transformation/silver_integrada.py

# 4. Camada Gold
python src/gold/gold_ranking_estados.py
python src/gold/gold_metas_vs_resultados.py
python src/gold/gold_painel_nacional.py

# 5. Streaming
python src/streaming/producer_simulado.py
python src/streaming/consumer_simulado.py

# 6. Monitoramento
python src/quality/pipeline_monitor.py
```

> ⚠️ As credenciais AWS Academy expiram por sessão — atualize o `.env` ao iniciar uma nova sessão.

---

## 📊 Principais Resultados

```
Taxa nacional de alfabetização (2025): 66.15%
Crianças avaliadas:                    1.969.921
Crianças alfabetizadas:                1.303.038
Meta 2030:                             100%
Distância da meta:                     33.85 pontos percentuais

Melhor estado:  Goiás — GO        (84.09%)
Pior estado:    Roraima — RR      (39.24%)
Diferença:      44.85 pontos percentuais

Municípios em nível crítico (<40%): 404 (3.25%)
```

📄 Análise completa por estado, município e região em [`docs/documentacao_tecnica.pdf`](docs/documentacao_tecnica.pdf)

---

## 💰 FinOps — Otimização de Custos

| Estratégia | Impacto |
|---|---|
| Formato Parquet | 83% de redução no armazenamento (261 MB → 45 MB) |
| Particionamento por data | Permite deletar partições antigas sem afetar dados recentes |
| Arquitetura Serverless | Zero custo quando a pipeline não está em execução |

| Recurso | Volume | Custo Mensal |
|---|---|---|
| S3 Bronze | 261.45 MB | $0.006 |
| S3 Silver | 45.13 MB | $0.001 |
| S3 Gold | 0.97 MB | $0.00002 |
| **Total** | **308.04 MB MB** | **~$0.007/mês** |

---

## 🤖 Aplicação em Inteligência Artificial

A camada Gold foi projetada para alimentar diretamente modelos de Machine Learning:

- **Predição de alfabetização** — prever a taxa futura de um município com XGBoost/Random Forest usando features como `perc_alunos_alfabetizados`, `distancia_meta`, `esforco_por_ano` e `regiao`
- **Clustering de municípios** — agrupar municípios com perfis similares (K-Means/DBSCAN) para priorização de políticas públicas
- **Alertas inteligentes** — usar o Streaming para detectar quedas bruscas no indicador e acionar alertas automáticos

📄 Detalhes das aplicações de IA em [`docs/documentacao_tecnica.pdf`](docs/documentacao_tecnica.pdf)

---

## 👨‍💻 Autor

**Guilherme Diniz Leocadio**  
FIAP PosTech — AI Scientist · Tech Challenge Fase 2 · 2026
