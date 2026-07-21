# Case Técnico Data Architect - iFood — Solução com PySpark

Solução ponta a ponta para ingestão, modelagem e disponibilização dos dados
de corrida de yellow taxi da NYC TLC (Jan–Mai/2023), usando **PySpark** e
arquitetura em camadas (medallion) pensada para rodar no **Databricks
Community Edition**, mas portável para qualquer cluster Spark.

## Sumário
- [Case Técnico Data Architect - iFood — Solução com PySpark](#case-técnico-data-architect---ifood--solução-com-pyspark)
  - [Sumário](#sumário)
  - [Arquitetura](#arquitetura)
  - [Decisões técnicas e justificativas](#decisões-técnicas-e-justificativas)
  - [Estrutura do repositório](#estrutura-do-repositório)
  - [Como executar](#como-executar)
    - [Opção A — Databricks Community Edition](#opção-a--databricks-community-edition)
    - [Opção B — Ambiente local](#opção-b--ambiente-local)
    - [Testes](#testes)
  - [Respostas às perguntas de análise](#respostas-às-perguntas-de-análise)
  - [Possíveis evoluções (fora do escopo atual + cenáario de produção)](#possíveis-evoluções-fora-do-escopo-atual--cenáario-de-produção)

## Arquitetura

```
                 raw_layer.py
NYC TLC (HTTPS) ───────────────────────▶ LANDING ZONE  (parquet original, as-is)
                                              │  particionado year=YYYY/month=MM
                                              │
                                       bronze_layer.py  (PySpark)
                                              │  + normalização de schema
                                              │  + metadados de ingestão
                                              ▼
                                          BRONZE         (Delta, "as-is" + lineage)
                                              │  particionado year/month
                                              │
                                       silver_layer.py  (PySpark)
                                              │  + regras de qualidade
                                              │  + tipagem/seleção de colunas
                                              ▼
                                     SILVER / CONSUMO    (Delta, tabela SQL)
                                              │  particionado pickup_year/pickup_month
                                              ▼
                                  consumo.yellow_tripdata  ──▶  SQL / BI / analysis/*.py
```

Três camadas, cada uma com uma responsabilidade única:

| Camada | Conteúdo | Por quê |
|---|---|---|
| **Landing** | Arquivos parquet exatamente como publicados pela TLC | Fonte da verdade bruta; permite reprocessar tudo do zero sem depender da disponibilidade futura do site da TLC |
| **Bronze** | Mesmos dados + metadados técnicos (`_source_file`, `_ingestion_timestamp`) | Schema já unificado entre meses, mas sem nenhuma regra de negócio aplicada — útil para auditoria e reprocessamento |
| **Silver** | Apenas as colunas exigidas (+ algumas úteis), tipadas e limpas | Contrato estável para os usuários finais consumirem via SQL |

## Decisões técnicas e justificativas

**PySpark + Delta Lake.** PySpark é exigência do case e é o padrão de
mercado para workloads deste volume (paralelismo nativo, boa integração
com Databricks). Delta Lake foi escolhido como formato das camadas Bronze
e Silver porque é o formato "de fábrica" do Databricks (zero configuração
extra em produção) e traz transações ACID, schema evolution e time
travel — relevante quando a TLC republica um mês com correções. O formato
é configurável via `IFOOD_TABLE_FORMAT` (ver [nota abaixo](#nota-sobre-validação-neste-ambiente)).

**Por que 3 camadas em vez de ingerir direto para uma tabela final?**
Desacopla a parte "técnica" (schema, tipos, lineage — Bronze) da parte
"de negócio" (quais colunas/regras a área de consumo precisa — Silver).
Se amanhã uma nova regra de qualidade for necessária, ela é reprocessada
só a partir da Bronze, sem precisar rebaixar nada da internet.

**Particionamento diferente em cada camada.** Landing/Bronze são
particionadas por `year/month` **do arquivo de origem** (alinhado à
publicação mensal da TLC — cada ingestão mexe em uma partição só,
idempotente). A Silver é particionada por `pickup_year/pickup_month`
**da data de negócio** (`tpep_pickup_datetime`), que é o padrão de
partition pruning correto para consultas típicas ("me dê Maio/2023"),
mesmo que um arquivo mensal contenha alguma corrida com erro de data.

**Normalização de nomes de coluna na Bronze.** A NYC TLC mudou a
capitalização de colunas entre meses de 2023 (ex.: `Airport_fee` vs.
`airport_fee`), o que quebra o `mergeSchema` do Spark ao ler vários
meses juntos. Resolvido normalizando tudo para lowercase na leitura da
Bronze; a Silver expõe de volta os nomes canônicos exigidos pelo case
(`VendorID`, não `vendorid`).

**Regras de qualidade da Silver** :
1. Remove linhas com `tpep_pickup_datetime`/`tpep_dropoff_datetime` nulos.
2. Remove corridas com `dropoff < pickup` (fisicamente impossível).
3. Remove `pickup` fora da janela Jan–Mai/2023 (a TLC publica uma fração
   de registros com datas claramente erradas, ex. ano 2002).
4. **Mantém** `total_amount` negativo — representa estornos/ajustes reais
   do negócio, não erro de captura; removê-los enviesaria a média.
5. Deduplica linhas 100% idênticas.

Cada regra é coberta por um teste unitário isolado em
`tests/test_silver_layer.py`.

**Camada de consumo via SQL.** `bronze_layer.py` e `silver_layer.py`
registram as tabelas no catálogo (`CREATE TABLE ... USING DELTA LOCATION
...`), então qualquer usuário final consulta com `SELECT * FROM
consumo.yellow_tripdata` — via notebook `%sql`, Databricks SQL Warehouse
ou qualquer cliente JDBC/ODBC — sem precisar saber PySpark.

## Estrutura do repositório

```
ifood-case/
├── src/
│   ├── config.py                     # paths, colunas obrigatórias, formato de tabela
│   ├── run_pipeline.py               # orquestra Bronze -> Silver
│   ├── ingestion/
│   │   ├── raw_layer.py      # Etapa 1: TLC -> Landing Zone
│   │   └── bronze_layer.py           # Etapa 2: Landing -> Bronze (PySpark)
│   ├── transformation/
│   │   └── silver_layer.py           # Etapa 3: Bronze -> Silver/Consumo (PySpark)
│   └── utils/
│       └── spark_session.py          # cria SparkSession (local ou Databricks)
├── analysis/
│   ├── 00_analise_exploratoria.py           # volumetria, nulos, estatísticas
│   ├── 01_media_total_amount_por_mes.py     # Pergunta 1
│   └── 02_media_passageiros_por_hora_maio.py # Pergunta 2
├── tests/
│   ├── test_silver_layer.py          # testes unitários das regras de qualidade
│   └── generate_sample_data.py       # gera dado sintético p/ validação offline
├── data_lake/                        # landing/bronze/silver locais (vazio no repo)
├── requirements.txt
└── README.md
```

## Como executar

### Opção A — Databricks Community Edition 

1. Crie um cluster no [Databricks Community Edition](https://community.cloud.databricks.com/)
2. Importe a pasta `src/` e `analysis/` no seu Workspace (via Repos, apontando
   para este repositório Git, ou por upload direto dos arquivos).
3. Rode em sequência, em um notebook (ou como Databricks Workflow com 3 tasks):
   ```python
   from src.ingestion import raw_layer, bronze_layer
   from src.transformation import silver_layer

   raw_layer.run()   # baixa os 5 arquivos oficiais da TLC
   bronze_layer.run()        # Landing -> Bronze
   silver_layer.run()        # Bronze -> Silver/Consumo
   ```
4. Consuma via SQL em qualquer notebook:
   ```sql
   SELECT * FROM consumo.yellow_tripdata LIMIT 10;
   ```
5. Rode os notebooks de `analysis/` para obter as respostas das perguntas.

### Opção B — Ambiente local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python -m src.ingestion.raw_layer   # baixa ~250 MB da TLC (5 meses)
python -m src.run_pipeline                  # Bronze + Silver
python -m analysis.00_analise_exploratoria
python -m analysis.01_media_total_amount_por_mes
python -m analysis.02_media_passageiros_por_hora_maio
```

> Requer Java 11+ instalado (dependência do PySpark) e acesso de rede ao
> Maven Central na primeira execução, para o `delta-spark` baixar o
> runtime do Delta Lake.

### Testes

```bash
pytest tests/test_silver_layer.py -v
```

## Respostas às perguntas de análise

O código das duas perguntas está em `analysis/01_media_total_amount_por_mes.py`
e `analysis/02_media_passageiros_por_hora_maio.py`, executado 100% via Spark
SQL sobre a tabela de consumo (`consumo.yellow_tripdata`).

**Pergunta 1 — Média de `total_amount` por mês, todos os yellow taxis:**
calculado com `AVG(total_amount)` agrupado por `pickup_year, pickup_month`.
Reportamos por mês (em vez de um único número) porque o valor varia
mês a mês — uma média única esconderia a sazonalidade.

**Pergunta 2 — Média de `passenger_count` por hora do dia, em maio/2023,
todos os yellow taxis:** calculado com `AVG(passenger_count)` agrupado
por `HOUR(tpep_pickup_datetime)`, filtrando `pickup_year=2023 AND
pickup_month=5`. "Todos os táxis da frota" é interpretado como "toda a
frota de yellow taxi ingerida" — o case define o schema/colunas do
dataset *yellow* como escopo; caso o escopo real inclua green/FHV/FHVHV,
basta repetir a ingestão para esses datasets e fazer `UNION` na consulta.



## Possíveis evoluções (fora do escopo atual + cenáario de produção)

**Orquestração e agendamento**
- Trocar as chamadas manuais (`raw_layer.run()` num notebook) por um **Databricks Workflow** com 3 tasks (Landing → Bronze → Silver) com dependência sequencial (ou **Airflow** usando a mesma lógica), retry automático e alertas de falha (e-mail/Slack via webhook).
- Agendamento mensal (a TLC publica um mês novo por vez) em vez de rodar tudo de uma vez — cada execução processa só o mês mais recente.
- Parametrizar o notebook/job por `year`/`month` em vez de hardcoded `MONTHS = [1,2,3,4,5]`, para reprocessar um mês específico sob demanda.

**Ingestão incremental (o maior gap do código atual)**
- Hoje cada camada faz `mode("overwrite")` do zero. Em produção, isso não escala — o certo é **Auto Loader** (`cloudFiles`) na Landing→Bronze, que detecta só os arquivos novos automaticamente, e `MERGE INTO` (upsert) na Bronze→Silver em vez de overwrite completo, para o caso da TLC republicar um mês com correção.
- Isso também elimina o `df.count()` completo que uso nos logs (caro em volume real — bom para debug, ruim em produção contínua).

**Armazenamento e governança (Unity Catalog "de verdade")**
- O que usamos aqui (Volume `/Volumes/workspace/default/...`) foi uma adaptação pro Free Edition. Em Enterprise, o certo é uma **External Location** registrada (S3/ADLS) com **Storage Credential** gerenciada por IAM/Service Principal — não um Volume genérico do catálogo `workspace`.
- Catálogos separados por ambiente: `dev`, `staging`, `prod` (ou um catálogo por domínio de dados), em vez de tudo dentro de `workspace`.
- **Grants** explícitos por schema/tabela (`GRANT SELECT ON consumo.yellow_tripdata TO ...`) em vez de qualquer usuário do workspace enxergar tudo.

**Qualidade de dado e observabilidade**
- As regras de qualidade hoje só logam `%` de linhas removidas. Em produção, isso vira **Delta Live Tables expectations** ou **Great Expectations**, com alerta automático se o `%` de descarte fugir do histórico.
- Lineage automático (Unity Catalog já rastreia isso nativamente quando as tabelas são criadas via Spark) para responder "de onde veio essa linha" sem depender só do `_source_file` manual feito aqui.

**CI/CD e ambientes**
- Código em **Databricks Asset Bundles** (ou Repos + CI externo), com testes (`pytest tests/`) rodando em pipeline antes de promover pra produção — hoje isso é manual.
- Separação clara dev → staging → prod, com o mesmo Job definido por config (YAML do Asset Bundle), não por notebook editado à mão.

**Performance e custo**
- **Job clusters** efêmeros (sobem só pra rodar o job e desligam) em vez de cluster interativo — muito mais barato pra carga agendada.
- `OPTIMIZE` + `Z-ORDER` (ou Liquid Clustering) periódico nas tabelas Delta, principalmente na Silver, que é a mais consultada.
- Revisitar o particionamento: `pickup_year/pickup_month` faz sentido pro volume do case (5 meses), mas em anos de histórico acumulado, particionar só por mês pode gerar partições pequenas demais — vale considerar Liquid Clustering em vez de particionamento fixo.

**Segurança**
- A URL da TLC é pública, então não há credencial aqui — mas em qualquer fonte real com autenticação, usar **Databricks Secrets** (`dbutils.secrets`), nunca hardcoded.
