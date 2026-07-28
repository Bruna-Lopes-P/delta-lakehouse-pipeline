"""Quarentena e integridade referencial.

Registro rejeitado não é descartado: vai para a quarentena com o motivo e a
linha original, para responder o que foi perdido sem reprocessar a origem.

Órfão cai na mesma quarentena: erro de vínculo tem o mesmo tratamento que erro
de formato.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.settings import Settings
from src.utils.logging import obter_logger
from src.utils.spark import gravar_delta, tabela_existe

logger = obter_logger("quarentena")


def separar_orfaos(
    df_filho: DataFrame,
    df_pai: DataFrame,
    chave_estrangeira: str,
    chave_pai: str,
    nome_motivo: str,
) -> tuple[DataFrame, DataFrame]:
    """Separa registros cuja chave estrangeira não existe no pai.

    Left anti join em vez de left join com filtro por nulo: só precisa decidir
    presença, não trazer coluna.

    Args:
        df_filho: entidade que referência.
        df_pai: entidade referenciada.
        chave_estrangeira: coluna em ``df_filho``.
        chave_pai: coluna em ``df_pai``.
        nome_motivo: motivo gravado na quarentena.

    Returns:
        (integros, órfãos). ``orfaos`` ganha a coluna ``motivos_rejeicao``.
    """
    pai_distinto = df_pai.select(F.col(chave_pai).alias("_chave_pai")).distinct()

    condicao = df_filho[chave_estrangeira] == pai_distinto["_chave_pai"]

    integros = df_filho.join(pai_distinto, condicao, "left_semi")
    orfaos = df_filho.join(pai_distinto, condicao, "left_anti").withColumn(
        "motivos_rejeicao", F.array(F.lit(nome_motivo))
    )

    return integros, orfaos


def gravar_quarentena(
    df_rejeitados: DataFrame,
    entidade: str,
    settings: Settings,
    camada_origem: str = "silver",
) -> int:
    """Grava rejeitados na tabela de quarentena.

    A linha original vira JSON em uma única coluna. Entidades diferentes tem
    schemas diferentes; serializar evita uma tabela de quarentena por entidade e
    mantém o dado recuperável.

    MERGE por ``hash_rejeicao`` em vez de append: a Prata reprocessa a Bronze
    inteira, então os mesmos inválidos reprovam de novo a cada execução.

    Returns:
        Quantidade de rejeições distintas nesta carga.
    """
    if "motivos_rejeicao" not in df_rejeitados.columns:
        raise ValueError(
            f"df de quarentena de {entidade!r} nao tem a coluna motivos_rejeicao. "
            "Use aplicar_regras ou separar_orfaos antes de gravar."
        )

    colunas_originais = [c for c in df_rejeitados.columns if c != "motivos_rejeicao"]

    df_quarentena = (
        df_rejeitados.select(
            F.lit(entidade).alias("entidade"),
            F.lit(camada_origem).alias("camada_origem"),
            F.lit(settings.batch_id).alias("batch_id"),
            F.lit(settings.data_referencia.isoformat()).alias("data_referencia"),
            F.current_timestamp().alias("quarentenado_em"),
            F.col("motivos_rejeicao"),
            F.to_json(F.struct(*colunas_originais)).alias("registro_original"),
        )
        .withColumn(
            "hash_rejeicao",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("entidade"),
                    F.col("registro_original"),
                    F.array_join(F.array_sort(F.col("motivos_rejeicao")), ","),
                ),
                256,
            ),
        )
        # A mesma rejeição pode aparecer duas vezes na própria carga quando a
        # origem repete a linha; o MERGE não aceita duas fontes para um alvo.
        .dropDuplicates(["hash_rejeicao"])
    )

    quantidade = df_quarentena.count()
    if quantidade == 0:
        return 0

    destino = settings.tabela("quarentena", "registros_rejeitados")

    if not tabela_existe(df_rejeitados.sparkSession, destino, settings):
        gravar_delta(df_quarentena, destino, settings, modo="overwrite")
    else:
        from delta.tables import DeltaTable

        alvo = (
            DeltaTable.forName(df_rejeitados.sparkSession, destino)
            if settings.usar_unity_catalog
            else DeltaTable.forPath(df_rejeitados.sparkSession, destino)
        )
        (
            alvo.alias("alvo")
            .merge(df_quarentena.alias("origem"), "alvo.hash_rejeicao = origem.hash_rejeicao")
            .whenNotMatchedInsertAll()
            .execute()
        )

    por_motivo = (
        df_quarentena.select(F.explode("motivos_rejeicao").alias("motivo"))
        .groupBy("motivo")
        .count()
        .collect()
    )

    logger.warning(
        "registros enviados para quarentena",
        extra={
            "entidade": entidade,
            "batch_id": settings.batch_id,
            "quantidade": quantidade,
            "por_motivo": {linha["motivo"]: linha["count"] for linha in por_motivo},
        },
    )

    return quantidade
