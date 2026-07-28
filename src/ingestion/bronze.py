"""Camada Bronze: ingestão incremental preservando o dado bruto.

Não limpa, não tipa e não descarta: guarda o que chegou com os metadados de
auditoria. Le tudo como string, porque o ``inferSchema`` muda de decisão entre
arquivos. Idempotência pelo controle de ``arquivo_origem`` já ingerido.

``ingerir_auto_loader`` é o caminho de produção; o batch existe para a suíte
rodar sem storage em nuvem.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.settings import Settings
from src.utils.logging import obter_logger
from src.utils.spark import gravar_delta, ler_delta, tabela_existe

logger = obter_logger("bronze")

COLUNAS_METADADOS = [
    "arquivo_origem",
    "data_ingestao",
    "timestamp_ingestao",
    "batch_id",
    "hash_linha",
    "schema_version",
]


def _colunas_de_negocio(df: DataFrame) -> list[str]:
    """Colunas do dado em si, sem os metadados de ingestão."""
    return [c for c in df.columns if c not in COLUNAS_METADADOS]


def _versao_schema(colunas: Sequence[str]) -> str:
    """Identificador estável do layout do arquivo.

    Derivado das colunas ordenadas. Quando a origem acrescenta um campo, a versão
    muda e fica registrada na linha, o que permite responder depois "a partir de
    qual carga esse campo passou a existir".
    """
    import hashlib

    assinatura = "|".join(sorted(colunas))
    return hashlib.sha256(assinatura.encode("utf-8")).hexdigest()[:12]


def adicionar_metadados(df: DataFrame, arquivo_origem: str, settings: Settings) -> DataFrame:
    """Anexa os metadados de auditoria a cada linha.

    ``hash_linha`` cobre apenas as colunas de negocio. Se cobrisse os metadados,
    a mesma linha lida em dois batches teria hashes diferentes e a deduplicação
    da Prata não funcionaria.
    """
    colunas = _colunas_de_negocio(df)

    return df.select(
        "*",
        F.lit(arquivo_origem).alias("arquivo_origem"),
        F.lit(settings.data_referencia.isoformat()).cast("date").alias("data_ingestao"),
        F.current_timestamp().alias("timestamp_ingestao"),
        F.lit(settings.batch_id).alias("batch_id"),
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<nulo>")) for c in colunas]), 256).alias("hash_linha"),
        F.lit(_versao_schema(colunas)).alias("schema_version"),
    )


def arquivos_ja_ingeridos(spark: SparkSession, entidade: str, settings: Settings) -> list[str]:
    """Arquivos que já entraram na Bronze dessa entidade."""
    destino = settings.tabela("bronze", entidade)

    if not tabela_existe(spark, destino, settings):
        return []

    linhas = (
        ler_delta(spark, destino, settings)
        .select("arquivo_origem")
        .distinct()
        .collect()
    )
    return [linha["arquivo_origem"] for linha in linhas]


def _listar_arquivos(spark: SparkSession, diretorio: str, extensao: str) -> list[str]:
    """Lista arquivos do diretório de pouso.

    Usa o Hadoop FileSystem da própria sessão, o que funciona igual em disco
    local, DBFS, S3 e ADLS.
    """
    jvm = spark._jvm
    caminho = jvm.org.apache.hadoop.fs.Path(diretorio)
    fs = caminho.getFileSystem(spark._jsc.hadoopConfiguration())

    if not fs.exists(caminho):
        return []

    encontrados = []
    for status in fs.listStatus(caminho):
        nome = status.getPath().getName()
        if status.isFile() and nome.endswith(extensao) and not nome.startswith("_"):
            encontrados.append(status.getPath().toString())

    return sorted(encontrados)


def ingerir(
    spark: SparkSession,
    entidade: str,
    origem: str,
    settings: Settings,
    formato: str = "csv",
    forcar_reingestao: bool = False,
) -> DataFrame | None:
    """Ingere os arquivos ainda não processados de uma origem.

    Args:
        spark: sessão ativa.
        entidade: nome da tabela Bronze de destino.
        origem: subdiretorio da landing.
        settings: configuração da execução.
        formato: csv ou json.
        forcar_reingestao: ignora o controle de arquivos já processados.

    Returns:
        DataFrame do que foi ingerido, ou None quando não havia arquivo novo.
    """
    diretorio = settings.landing(origem)
    extensao = ".json" if formato == "json" else ".csv"
    arquivos = _listar_arquivos(spark, diretorio, extensao)

    if not arquivos:
        logger.warning("nenhum arquivo encontrado", extra={"entidade": entidade, "diretorio": diretorio})
        return None

    if forcar_reingestao:
        pendentes = arquivos
    else:
        ja_vistos = set(arquivos_ja_ingeridos(spark, entidade, settings))
        pendentes = [a for a in arquivos if a not in ja_vistos]

    if not pendentes:
        logger.info(
            "nenhum arquivo novo, ingestao ignorada",
            extra={"entidade": entidade, "arquivos_no_diretorio": len(arquivos)},
        )
        return None

    leitor = spark.read.option("header", "true").option("inferSchema", "false")
    if formato == "json":
        leitor = leitor.format("json")
    else:
        leitor = leitor.format("csv").option("mode", "PERMISSIVE")

    frames = []
    for arquivo in pendentes:
        # Um arquivo por vez porque arquivo_origem precisa refletir a procedência
        # real de cada linha. Ler o diretório inteiro de uma vez perderia isso.
        df_arquivo = leitor.load(arquivo)
        frames.append(adicionar_metadados(df_arquivo, arquivo, settings))

    # unionByName com allowMissingColumns absorve a evolução de schema: o arquivo
    # que ganhou coluna nova entra junto, e as linhas antigas ficam com nulo nela.
    df = frames[0]
    for outro in frames[1:]:
        df = df.unionByName(outro, allowMissingColumns=True)

    destino = settings.tabela("bronze", entidade)
    gravar_delta(df, destino, settings, modo="append")

    quantidade = df.count()
    logger.info(
        "ingestao bronze concluida",
        extra={
            "entidade": entidade,
            "batch_id": settings.batch_id,
            "arquivos_novos": len(pendentes),
            "arquivos_ignorados": len(arquivos) - len(pendentes),
            "linhas": quantidade,
            "destino": destino,
        },
    )

    return df


def ingerir_auto_loader(
    spark: SparkSession,
    entidade: str,
    origem: str,
    settings: Settings,
    formato: str = "csv",
) -> None:
    """Ingestão com Auto Loader. Caminho de produção.

    ``availableNow`` processa o backlog e encerra, sem manter cluster de
    streaming ligado. Requer storage em nuvem, então não roda na suíte local.
    """
    diretorio = settings.landing(origem)
    checkpoint = settings.checkpoint(origem)
    destino = settings.tabela("bronze", entidade)

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .load(diretorio)
    )

    com_metadados = (
        stream.withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("data_ingestao", F.lit(settings.data_referencia.isoformat()).cast("date"))
        .withColumn("timestamp_ingestao", F.current_timestamp())
        .withColumn("batch_id", F.lit(settings.batch_id))
    )

    escritor = (
        com_metadados.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint}/commits")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .outputMode("append")
    )

    consulta = escritor.toTable(destino) if settings.usar_unity_catalog else escritor.start(destino)
    consulta.awaitTermination()

    logger.info(
        "ingestao auto loader concluida",
        extra={"entidade": entidade, "batch_id": settings.batch_id, "destino": destino},
    )
