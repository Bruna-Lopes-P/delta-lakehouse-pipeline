"""Fatos da Prata: transação, estorno e evento de risco.

Fato não versiona: MERGE por chave natural. Correção chega como estorno, que é
outro fato.

Particionado por ``data_particao``, a data da transação e não a da carga.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config.settings import Settings
from src.quality.quarantine import gravar_quarentena, separar_orfaos
from src.quality.rules import (
    aplicar_regras,
    regras_estorno,
    regras_evento_risco,
    regras_transacao,
)
from src.silver.scd import versao_corrente
from src.silver.tipos import converter, texto, texto_padronizado
from src.utils.logging import obter_logger
from src.utils.spark import gravar_delta, ler_delta, tabela_existe

logger = obter_logger("silver.fatos")


def tipar_transacao(df_bronze: DataFrame) -> DataFrame:
    """Converte a transação bruta para os tipos do modelo.

    ``dispositivo`` só existe nos arquivos mais recentes. O acesso condicional
    evita que a carga quebre quando a coluna ainda não apareceu, mantendo a
    compatibilidade com o histórico.
    """
    dispositivo = (
        texto_padronizado("dispositivo")
        if "dispositivo" in df_bronze.columns
        else F.lit(None).cast("string")
    )

    return df_bronze.select(
        converter("id_transacao", "bigint").alias("id_transacao"),
        converter("id_cartao", "bigint").alias("id_cartao"),
        converter("data_transacao", "timestamp").alias("data_transacao"),
        converter("valor", "decimal(18,2)").alias("valor"),
        converter("mcc", "int").alias("mcc"),
        texto("estabelecimento").alias("estabelecimento"),
        texto_padronizado("canal").alias("canal"),
        texto_padronizado("pais").alias("pais"),
        texto_padronizado("moeda").alias("moeda"),
        dispositivo.alias("dispositivo"),
        converter("data_transacao", "date").alias("data_particao"),
        F.col("arquivo_origem"),
        F.col("batch_id"),
        F.col("timestamp_ingestao"),
    )


def deduplicar_por_chave(
    df: DataFrame, chave: Sequence[str], coluna_desempate: str = "timestamp_ingestao"
) -> DataFrame:
    """Mantém uma linha por chave, a de ingestão mais recente.

    A mesma transação pode aparecer em dois arquivos quando a origem reenvia um
    período. Vence a versão ingerida por último, que é a correção mais recente.
    """
    janela = Window.partitionBy(*chave).orderBy(F.col(coluna_desempate).desc())
    return (
        df.withColumn("_ordem", F.row_number().over(janela))
        .filter(F.col("_ordem") == 1)
        .drop("_ordem")
    )


def _merge_por_chave(
    spark: SparkSession,
    df: DataFrame,
    entidade: str,
    settings: Settings,
    chave: Sequence[str],
    particoes: Sequence[str] = (),
) -> int:
    """Insere o que é novo e atualiza o que mudou, sem duplicar.

    Primeira carga grava direto; as seguintes usam MERGE. O UPDATE existe porque
    a origem pode reenviar uma linha corrigida com a mesma chave.
    """
    destino = settings.tabela("silver", entidade)

    if not tabela_existe(spark, destino, settings):
        gravar_delta(df, destino, settings, modo="overwrite", particoes=list(particoes) or None)
        return df.count()

    from delta.tables import DeltaTable

    alvo = (
        DeltaTable.forName(spark, destino)
        if settings.usar_unity_catalog
        else DeltaTable.forPath(spark, destino)
    )
    condicao = " AND ".join([f"alvo.{c} = origem.{c}" for c in chave])

    (
        alvo.alias("alvo")
        .merge(df.alias("origem"), condicao)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    return df.count()


def processar_transacao(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Bronze de transação para fato Prata deduplicado."""
    bronze = ler_delta(spark, settings.tabela("bronze", "transacoes"), settings)
    tipado = tipar_transacao(bronze)
    lidos = tipado.count()

    aprovados, rejeitados = aplicar_regras(tipado, regras_transacao())
    rejeitados_regra = gravar_quarentena(rejeitados, "transacao", settings)

    dim_cartao = versao_corrente(
        ler_delta(spark, settings.tabela("silver", "dim_cartao"), settings)
    )
    integros, orfaos = separar_orfaos(
        df_filho=aprovados,
        df_pai=dim_cartao,
        chave_estrangeira="id_cartao",
        chave_pai="id_cartao",
        nome_motivo="transacao_sem_cartao_valido",
    )
    rejeitados_orfao = gravar_quarentena(orfaos, "transacao", settings)

    dedup = deduplicar_por_chave(integros, ["id_transacao"])
    duplicatas = integros.count() - dedup.count()

    gravados = _merge_por_chave(
        spark, dedup, "fato_transacao", settings,
        chave=["id_transacao"],
        particoes=["data_particao"],
    )

    logger.info(
        "fato transacao processado",
        extra={
            "batch_id": settings.batch_id,
            "lidos": lidos,
            "rejeitados_regra": rejeitados_regra,
            "rejeitados_orfaos": rejeitados_orfao,
            "duplicatas_removidas": duplicatas,
            "gravados": gravados,
        },
    )

    return {
        "lidos": lidos,
        "rejeitados": rejeitados_regra + rejeitados_orfao,
        "duplicatas_removidas": duplicatas,
        "gravados": gravados,
    }


def processar_estorno(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Bronze de estorno para fato Prata.

    Estorno que aponta para transação inexistente vai para quarentena. Sem a
    transação original não ha valor bruto do qual subtrair, e deixar passar
    produziria valor liquido negativo sem contrapartida.
    """
    bronze = ler_delta(spark, settings.tabela("bronze", "estornos"), settings)

    tipado = bronze.select(
        converter("id_estorno", "bigint").alias("id_estorno"),
        converter("id_transacao", "bigint").alias("id_transacao"),
        converter("data_estorno", "date").alias("data_estorno"),
        converter("valor_estorno", "decimal(18,2)").alias("valor_estorno"),
        F.upper(F.trim(F.col("motivo"))).alias("motivo"),
        F.col("arquivo_origem"),
        F.col("batch_id"),
        F.col("timestamp_ingestao"),
    )
    lidos = tipado.count()

    aprovados, rejeitados = aplicar_regras(tipado, regras_estorno())
    rejeitados_regra = gravar_quarentena(rejeitados, "estorno", settings)

    fato_transacao = ler_delta(spark, settings.tabela("silver", "fato_transacao"), settings)
    integros, orfaos = separar_orfaos(
        df_filho=aprovados,
        df_pai=fato_transacao,
        chave_estrangeira="id_transacao",
        chave_pai="id_transacao",
        nome_motivo="estorno_sem_transacao_valida",
    )
    rejeitados_orfao = gravar_quarentena(orfaos, "estorno", settings)

    dedup = deduplicar_por_chave(integros, ["id_estorno"])
    gravados = _merge_por_chave(spark, dedup, "fato_estorno", settings, chave=["id_estorno"])

    return {
        "lidos": lidos,
        "rejeitados": rejeitados_regra + rejeitados_orfao,
        "gravados": gravados,
    }


def processar_evento_risco(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Bronze de evento de risco para fato Prata."""
    bronze = ler_delta(spark, settings.tabela("bronze", "eventos_risco"), settings)

    tipado = bronze.select(
        converter("id_evento", "bigint").alias("id_evento"),
        converter("id_transacao", "bigint").alias("id_transacao"),
        F.upper(F.trim(F.col("tipo_evento"))).alias("tipo_evento"),
        F.upper(F.trim(F.col("severidade"))).alias("severidade"),
        converter("data_evento", "date").alias("data_evento"),
        F.col("arquivo_origem"),
        F.col("batch_id"),
        F.col("timestamp_ingestao"),
    )
    lidos = tipado.count()

    aprovados, rejeitados = aplicar_regras(tipado, regras_evento_risco())
    rejeitados_regra = gravar_quarentena(rejeitados, "evento_risco", settings)

    fato_transacao = ler_delta(spark, settings.tabela("silver", "fato_transacao"), settings)
    integros, orfaos = separar_orfaos(
        df_filho=aprovados,
        df_pai=fato_transacao,
        chave_estrangeira="id_transacao",
        chave_pai="id_transacao",
        nome_motivo="evento_sem_transacao_valida",
    )
    rejeitados_orfao = gravar_quarentena(orfaos, "evento_risco", settings)

    dedup = deduplicar_por_chave(integros, ["id_evento"])
    gravados = _merge_por_chave(spark, dedup, "fato_evento_risco", settings, chave=["id_evento"])

    return {
        "lidos": lidos,
        "rejeitados": rejeitados_regra + rejeitados_orfao,
        "gravados": gravados,
    }
