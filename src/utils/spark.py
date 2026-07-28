"""Criação da SparkSession e acesso a Delta.

No Databricks a sessão já existe e e reaproveitada. Fora dele, monta uma sessão
local com as extensoes do Delta, o que permite rodar os testes sem cluster.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.config.settings import Settings


def obter_sessao(nome_app: str = "novarota-lakehouse") -> SparkSession:
    """Reaproveita a sessão ativa ou cria uma local com Delta habilitado."""
    ativa = SparkSession.getActiveSession()
    if ativa is not None:
        return ativa

    construtor = (
        SparkSession.builder.appName(nome_app)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # Defesa para o MERGE com updateAll/insertAll quando a origem traz
        # coluna ausente no destino.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    try:
        from delta import configure_spark_with_delta_pip

        return configure_spark_with_delta_pip(construtor).getOrCreate()
    except ImportError:
        return construtor.getOrCreate()


def gravar_delta(
    df: DataFrame,
    destino: str,
    settings: Settings,
    modo: str = "append",
    particoes: list[str] | None = None,
) -> None:
    """Grava em tabela gerenciada (UC) ou em path, conforme a configuração.

    Args:
        df: dados a gravar.
        destino: retorno de ``Settings.tabela``.
        settings: configuração da execução.
        modo: append ou overwrite.
        partições: colunas de particionamento fisico.
    """
    escritor = df.write.format("delta").mode(modo)

    if particoes:
        escritor = escritor.partitionBy(*particoes)

    if modo == "overwrite":
        escritor = escritor.option("overwriteSchema", "true")
    else:
        escritor = escritor.option("mergeSchema", "true")

    if settings.usar_unity_catalog:
        escritor.saveAsTable(destino)
    else:
        escritor.save(destino)


def ler_delta(spark: SparkSession, destino: str, settings: Settings) -> DataFrame:
    """Le uma tabela Delta gerenciada ou em path."""
    if settings.usar_unity_catalog:
        return spark.table(destino)
    return spark.read.format("delta").load(destino)


def tabela_existe(spark: SparkSession, destino: str, settings: Settings) -> bool:
    """Informa se o destino já foi materializado."""
    if settings.usar_unity_catalog:
        return spark.catalog.tableExists(destino)

    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, destino)


def otimizar(spark: SparkSession, destino: str, settings: Settings, zorder: list[str] | None = None) -> None:
    """Compacta arquivos pequenos e aplica Z-ORDER.

    Chamado ao fim de cada camada. Em Databricks Runtime o OPTIMIZE existe; em
    Delta OSS local o comando não está disponível e a chamada é ignorada, por
    isso o try/except restrito a AnalysisException.
    """
    from pyspark.sql.utils import AnalysisException

    alvo = destino if settings.usar_unity_catalog else f"delta.`{destino}`"
    comando = f"OPTIMIZE {alvo}"
    if zorder:
        comando += f" ZORDER BY ({', '.join(zorder)})"

    try:
        spark.sql(comando)
    except (AnalysisException, Exception) as erro:  # noqa: B014
        # OPTIMIZE não existe no Delta OSS; qualquer outro erro e propagado.
        if "OPTIMIZE" not in str(erro) and "ParseException" not in type(erro).__name__:
            raise
