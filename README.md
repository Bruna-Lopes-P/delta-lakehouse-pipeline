# Data Product Lakehouse

Pipeline transacional em arquitetura Medallion sobre Databricks e Delta Lake,
com carga incremental, SCD Tipo 2, quarentena de registros inválidos e
observabilidade por etapa.

## Arquitetura

```
ORIGENS (landing)
   clientes, contas, cartões, transações, estornos, eventos de risco
        |
        v
BRONZE    cópia fiel da origem, tudo como string
          metadados: arquivo_origem, data_ingestao, timestamp_ingestao,
          batch_id, hash_linha, schema_version
        |
        v
PRATA     dimensões com SCD Tipo 2 (cliente, conta, cartão)
          fatos com MERGE por chave (transação, estorno, evento de risco)
          rejeitados vão para quarentena com motivo
        |
        v
OURO      gold_fato_transacao, gold_cliente_mes, gold_indicadores_risco,
          gold_features_cliente, gold_dim_cliente/conta/cartao/estabelecimento
```

Detalhes em [arquitetura](docs/architecture.md),
[decisões](docs/decisions.md) e [contratos de dados](docs/data_contracts.md).

## Executar

Requer Python 3.9 a 3.11 (o PySpark 3.5 não suporta 3.12+) e Java 17 ou 21.

```bash
python -m venv .venv
.venv/Scripts/activate        # Windows
source .venv/bin/activate     # Linux e macOS

pip install -e ".[dev]"
```

No Windows, o Hadoop precisa do `winutils.exe` e do `hadoop.dll` para escrever
em disco local. Sem eles a gravação Delta falha com `UnsatisfiedLinkError`:

```bash
mkdir -p /c/hadoop/bin
curl -L -o /c/hadoop/bin/winutils.exe \
  https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe
curl -L -o /c/hadoop/bin/hadoop.dll \
  https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll
```

Rodar o pipeline gerando a massa sintética:

```bash
python -m src.pipeline.run --gerar-massa --ambiente dev --data-referencia 2026-03-31
```

Parâmetros: `--ambiente` (dev, hml, prd), `--modo` (completa, bronze, silver,
gold), `--data-referencia`, `--batch-id`, `--gerar-massa`, `--log`.

Saída em `data/lakehouse/`, origem em `data/landing/`.

### Databricks

Clonar em Repos, ajustar `RAIZ_PROJETO` em
[`notebooks/executar_pipeline.py`](notebooks/executar_pipeline.py) e executar.
Em Workflow, uma task por camada usando `--modo`.

Para tabelas gerenciadas no Unity Catalog, definir `usar_unity_catalog = sim`.

## Testes

```bash
pytest tests/test_quality_rules.py tests/test_scd.py tests/test_silver.py tests/test_gold.py
pytest tests/test_sql.py
pytest tests/test_pipeline.py
```

Os grupos são separados porque a sessão Spark é compartilhada e o driver acumula
metadados de todos os stages. O `conftest` sobe o driver com 4 GB.

| Arquivo | Cobre |
|---|---|
| `test_quality_rules.py` | Regras, motivo da rejeição, integridade referencial, quarentena |
| `test_scd.py` | Vigência, versão corrente, histórico, consulta pontual, idempotência, dado atrasado |
| `test_silver.py` | Tipagem, normalização, deduplicação por chave |
| `test_gold.py` | Estorno, cartão cancelado, join temporal, indicadores de risco |
| `test_sql.py` | Cada consulta de `sql/` submetida ao analisador do Spark |
| `test_pipeline.py` | Ponta a ponta, idempotência, invariantes sobre a massa completa |

## Evidências de execução

[`evidencias/`](evidencias/) tem o resultado de uma execução real: contagem por
tabela em duas execuções seguidas (prova de idempotência), métricas por etapa,
quarentena por motivo, invariantes verificadas e totais do negócio.

Para regenerar: `python -m scripts.gerar_evidencias`.

## Cenários de negócio cobertos

| Cenário | Tratamento | Teste |
|---|---|---|
| Cliente com várias contas e cartões | Modelagem da massa | `test_pipeline.py::TestPipelineCompleto` |
| Cartão muda de status | SCD Tipo 2 em `dim_cartao` | `test_scd.py::TestNovaVersao` |
| Cartão cancelado fora da métrica, histórico preservado | `elegivel_metrica` usa o status vigente na data | `test_gold.py::TestCartaoCancelado` |
| Transação estornada fora do líquido | `valor_liquido = valor_bruto - valor_estornado` | `test_gold.py::TestEstorno` |
| Cadastro vigente na data da transação | Join temporal com a dimensão | `test_gold.py::TestJoinTemporal` |
| Arquivos fora de ordem | Vigência vem da origem, registro superado é descartado | `test_scd.py::TestDadoAtrasado` |
| Mesmo `id_transacao` em cargas diferentes | Deduplicação por chave antes do MERGE | `test_silver.py::TestDeduplicacao` |

## SQL

| Arquivo | Conteúdo |
|---|---|
| [`01_analise_comportamental.sql`](sql/01_analise_comportamental.sql) | Anomalia de gasto, cliente contra o próprio histórico e contra o grupo, segmentação, curva ABC |
| [`02_scd_e_carga_incremental.sql`](sql/02_scd_e_carga_incremental.sql) | Consulta pontual, linha do tempo, invariantes, MERGE do SCD |
| [`03_observabilidade.sql`](sql/03_observabilidade.sql) | Resumo da execução, queda de volume, taxa de rejeição, frescor |

Usa CTEs encadeadas, `ROW_NUMBER`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`,
`NTILE`, `PERCENT_RANK`, `CUME_DIST`, `PERCENTILE_CONT`, `WINDOW` nomeada e
`MERGE INTO`.

## Estrutura

```
src/
  config/          parametrização da execução
  utils/           sessão Spark, gravação Delta, log JSON
  datagen/         massa sintética determinística
  ingestion/       Bronze
  quality/         regras, quarentena, integridade referencial
  silver/          SCD Tipo 2, dimensões, fatos
  gold/            data products
  observability/   métricas por etapa
  pipeline/        orquestração e CLI

tests/       suíte automatizada
sql/         consultas
notebooks/   execução no Databricks
scripts/     geração das evidências
evidencias/  resultado de uma execução
docs/        arquitetura, decisões, contratos
```

Camada não importa camada. O acoplamento fica em `pipeline/run.py`, e o notebook
é casca de execução sem regra de negócio.

## Premissas e limitações

Implementado como código, não exercitado pela suíte:

* Auto Loader. `ingerir_auto_loader` é o caminho de produção, mas depende de
  storage em nuvem. O pipeline usa a ingestão em batch, que tem o mesmo contrato
  de idempotência e roda em qualquer máquina.
* Unity Catalog. O código grava em tabela gerenciada quando
  `usar_unity_catalog` está ligado. A organização de catálogos e permissões está
  documentada, sem provisionamento.
* `OPTIMIZE` e `ZORDER`. Chamados ao fim das camadas Prata e Ouro. Em Delta OSS
  local o comando não existe e a chamada é ignorada.

Apenas documentado:

* Workflow do Databricks. O desenho das tasks está descrito, sem arquivo de job
  versionado.
* SLA. Alvos e consultas de medição existem, sem alerta configurado.

Escopo assumido:

* `data_referencia` é data lógica e não filtra a carga. Cada execução reprocessa
  a Bronze inteira, e a idempotência garante que isso não duplique.
* A Ouro é reconstruída por overwrite, o que vale enquanto o volume permitir.

## Próximos passos

1. CI rodando `ruff` e `pytest` a cada push.
2. Databricks Asset Bundles para versionar o Workflow.
3. Alertas ligados às consultas de observabilidade.
4. Mascaramento de CPF na Prata via column mask.
5. Liquid Clustering no lugar de partição mais Z-ORDER.
6. Reprocessamento por partição quando o volume não permitir overwrite.
