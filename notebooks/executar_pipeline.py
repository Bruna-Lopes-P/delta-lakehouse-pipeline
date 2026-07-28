# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline NovaRota
# MAGIC
# MAGIC Executa o data product transacional de ponta a ponta.
# MAGIC
# MAGIC O notebook é casca de execução: não contém regra de negócio. Tudo que ele
# MAGIC faz é ler os parâmetros do job, montar o `Settings` e chamar o pipeline.
# MAGIC A lógica vive em `src/`, onde é testável sem cluster.
# MAGIC
# MAGIC **Pré-requisito.** O repositório precisa estar clonado em Repos e a raiz
# MAGIC do projeto adicionada ao `sys.path` (célula seguinte).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros
# MAGIC
# MAGIC Widgets viram parâmetros da task no Workflow. Nenhum valor de ambiente
# MAGIC fica fixo no código.

# COMMAND ----------

dbutils.widgets.dropdown("ambiente", "dev", ["dev", "hml", "prd"])
dbutils.widgets.dropdown("modo", "completa", ["completa", "bronze", "silver", "gold"])
dbutils.widgets.text("data_referencia", "", "Data de referencia (AAAA-MM-DD)")
dbutils.widgets.text("batch_id", "", "Batch ID (vazio gera automatico)")
dbutils.widgets.dropdown("gerar_massa", "não", ["sim", "não"])
dbutils.widgets.text("catalogo", "novarota")
dbutils.widgets.text("raiz_dados", "/Volumes/novarota/default/lakehouse")
dbutils.widgets.text("raiz_landing", "/Volumes/novarota/default/landing")
dbutils.widgets.dropdown("usar_unity_catalog", "não", ["sim", "não"])

# COMMAND ----------

import sys
from datetime import date

# Ajuste conforme o caminho do repositorio em Repos.
RAIZ_PROJETO = "/Workspace/Repos/seu.usuario/delta-lakehouse-pipeline"
if RAIZ_PROJETO not in sys.path:
    sys.path.insert(0, RAIZ_PROJETO)

from src.config.settings import Settings
from src.pipeline.run import executar
from src.utils.logging import configurar_logs

configurar_logs("INFO")

# COMMAND ----------

texto_data = dbutils.widgets.get("data_referencia").strip()
texto_batch = dbutils.widgets.get("batch_id").strip()

campos = {
    "ambiente": dbutils.widgets.get("ambiente"),
    "modo_execucao": dbutils.widgets.get("modo"),
    "catalogo": dbutils.widgets.get("catalogo"),
    "raiz_dados": dbutils.widgets.get("raiz_dados"),
    "raiz_landing": dbutils.widgets.get("raiz_landing"),
    "usar_unity_catalog": dbutils.widgets.get("usar_unity_catalog") == "sim",
}

if texto_data:
    campos["data_referencia"] = date.fromisoformat(texto_data)
if texto_batch:
    campos["batch_id"] = texto_batch

settings = Settings(**campos)

print(f"ambiente         {settings.ambiente}")
print(f"modo             {settings.modo_execucao}")
print(f"data_referencia  {settings.data_referencia}")
print(f"batch_id         {settings.batch_id}")
print(f"unity_catalog    {settings.usar_unity_catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Massa sintética
# MAGIC
# MAGIC Apenas para demonstração em ambiente de desenvolvimento. A geração é
# MAGIC determinística: duas execuções produzem os mesmos arquivos.

# COMMAND ----------

if dbutils.widgets.get("gerar_massa") == "sim":
    if settings.ambiente == "prd":
        raise ValueError("geração de massa sintética não é permitida em produção")

    from src.datagen.generator import gerar_massa

    resumo = gerar_massa(settings, qtd_clientes=200, dias=5)
    print(resumo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução
# MAGIC
# MAGIC Falha em qualquer etapa interrompe o pipeline. As métricas coletadas até
# MAGIC o ponto da falha são persistidas mesmo assim.

# COMMAND ----------

resumo = executar(settings, spark)
resumo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Métricas da execução

# COMMAND ----------

from src.utils.spark import ler_delta

metricas = ler_delta(spark, settings.tabela("observabilidade", "execucao_pipeline"), settings)
display(
    metricas.filter(f"batch_id = '{settings.batch_id}'")
    .select("camada", "etapa", "entidade", "linhas_lidas", "linhas_gravadas",
            "linhas_rejeitadas", "duracao_segundos", "status")
    .orderBy("iniciado_em")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarentena
# MAGIC
# MAGIC Quantidade por motivo. Volume alto em um motivo único costuma indicar
# MAGIC mudança na origem, não dado ruim disperso.

# COMMAND ----------

from pyspark.sql import functions as F

quarentena = ler_delta(spark, settings.tabela("quarentena", "registros_rejeitados"), settings)
display(
    quarentena.filter(f"batch_id = '{settings.batch_id}'")
    .select("entidade", F.explode("motivos_rejeicao").alias("motivo"))
    .groupBy("entidade", "motivo")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plano de execução
# MAGIC
# MAGIC O que procurar na saída:
# MAGIC
# MAGIC - `BroadcastHashJoin` nos joins com dimensão. Se aparecer `SortMergeJoin`,
# MAGIC   a dimensão passou do limite de broadcast e o join com faixa de vigência
# MAGIC   vai custar shuffle nos dois lados.
# MAGIC - `PushedFilters` na leitura do Delta, confirmando que o filtro de data
# MAGIC   desceu até o scan em vez de ser aplicado depois.
# MAGIC - Número de partições lidas. Consulta com filtro de período que lê todas
# MAGIC   as partições indica que a poda não aconteceu.
# MAGIC
# MAGIC A análise completa está em `docs/architecture.md`.

# COMMAND ----------

from src.gold.products import construir_fato_transacao

# Plano da construção do fato, que é onde os quatro joins com dimensão acontecem.
construir_fato_transacao(spark, settings).explain(mode="formatted")

# COMMAND ----------

# Plano de uma consulta tipica de consumo, para conferir a poda de partição.
caminho_gold = settings.tabela("gold", "gold_fato_transacao")
alvo = caminho_gold if settings.usar_unity_catalog else f"delta.`{caminho_gold}`"

spark.sql(f"""
    SELECT id_cliente, SUM(valor_liquido) AS total
    FROM {alvo}
    WHERE data_particao >= DATE_SUB(DATE '{settings.data_referencia}', 7)
    GROUP BY id_cliente
""").explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação das invariantes
# MAGIC
# MAGIC Checagens que precisam retornar zero. Qualquer resultado diferente é
# MAGIC defeito na carga, não no dado.

# COMMAND ----------

for entidade, chave in [("dim_cliente", "id_cliente"), ("dim_conta", "id_conta"),
                        ("dim_cartao", "id_cartao")]:
    dim = ler_delta(spark, settings.tabela("silver", entidade), settings)

    duplicadas = (
        dim.filter("dw_versao_ativa")
        .groupBy(chave).count().filter("count > 1").count()
    )
    invertidas = dim.filter(
        "dw_fim_vigencia IS NOT NULL AND dw_fim_vigencia <= dw_inicio_vigencia"
    ).count()

    print(f"{entidade}: versões ativas duplicadas={duplicadas}, intervalos invertidos={invertidas}")
    assert duplicadas == 0, f"{entidade} tem chave com mais de uma versão ativa"
    assert invertidas == 0, f"{entidade} tem intervalo de vigência invertido"

fato = ler_delta(spark, settings.tabela("gold", "gold_fato_transacao"), settings)
totais = fato.agg(
    F.sum("valor_bruto").alias("bruto"),
    F.sum("valor_estornado").alias("estornado"),
    F.sum("valor_liquido").alias("liquido"),
).collect()[0]

print(f"bruto={totais['bruto']} estornado={totais['estornado']} liquido={totais['liquido']}")
assert totais["liquido"] == totais["bruto"] - totais["estornado"]

duplicadas_fato = fato.count() - fato.select("id_transacao").distinct().count()
print(f"transações duplicadas na Ouro: {duplicadas_fato}")
assert duplicadas_fato == 0

print("\ntodas as invariantes conferidas")
