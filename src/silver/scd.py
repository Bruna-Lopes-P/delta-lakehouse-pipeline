"""SCD Tipo 2 sobre Delta Lake.

Vigência em intervalo semiaberto ``[dw_inicio_vigencia, dw_fim_vigencia)``, com
fim nulo na versão corrente. O início vem da ``data_atualizacao`` da origem, não
de ``current_date()``.

Detalhes das escolhas em docs/decisions.md.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config.settings import Settings
from src.utils.logging import obter_logger
from src.utils.spark import gravar_delta, ler_delta, tabela_existe

logger = obter_logger("scd")

COLUNAS_TECNICAS = [
    "dw_hash_atributos",
    "dw_inicio_vigencia",
    "dw_fim_vigencia",
    "dw_versao_ativa",
    "dw_excluido",
    "dw_batch_id",
    "dw_atualizado_em",
]


def _hash_atributos(colunas: Sequence[str]) -> F.Column:
    """Hash estável dos atributos que definem uma versão.

    Nulo vira um marcador explícito para que ``(A, nulo)`` e ``(nulo, A)`` não
    colidam no mesmo hash, o que aconteceria com concatenação simples.
    """
    partes = [F.coalesce(F.col(c).cast("string"), F.lit("<nulo>")) for c in colunas]
    return F.sha2(F.concat_ws("||", *partes), 256)


def preparar_versoes(
    df: DataFrame,
    chave_negocio: Sequence[str],
    coluna_versao: str,
    colunas_atributos: Sequence[str],
    coluna_operacao: str | None = None,
) -> DataFrame:
    """Monta a linha do tempo da própria carga.

    Uma carga de CDC pode trazer varias versões da mesma chave; elas são
    encadeadas com ``LEAD`` e só a última fica ativa.
    """
    df = df.withColumn("dw_hash_atributos", _hash_atributos(colunas_atributos))

    if coluna_operacao and coluna_operacao in df.columns:
        df = df.withColumn(
            "dw_excluido",
            F.upper(F.coalesce(F.col(coluna_operacao), F.lit(""))) == F.lit("DELETE"),
        )
    else:
        df = df.withColumn("dw_excluido", F.lit(False))

    # Duplicata exata da mesma versão: mantém uma so.
    chaves_dedup = list(chave_negocio) + [coluna_versao, "dw_hash_atributos"]
    df = df.dropDuplicates(chaves_dedup)

    # Mesma chave e mesmo instante com conteúdo diferente: conflito real dentro
    # da carga. Vence o maior hash, critério arbitrário mas determinístico, o que
    # importa para a idempotência.
    janela_conflito = Window.partitionBy(*chave_negocio, coluna_versao).orderBy(
        F.col("dw_hash_atributos").desc()
    )
    df = (
        df.withColumn("_ordem_conflito", F.row_number().over(janela_conflito))
        .filter(F.col("_ordem_conflito") == 1)
        .drop("_ordem_conflito")
    )

    # Encadeia as versões da carga: cada uma termina onde a próxima começa.
    janela_tempo = Window.partitionBy(*chave_negocio).orderBy(F.col(coluna_versao).asc())
    df = df.withColumn("_proxima_vigencia", F.lead(coluna_versao).over(janela_tempo))

    return (
        df.withColumn("dw_inicio_vigencia", F.col(coluna_versao))
        .withColumn("dw_fim_vigencia", F.col("_proxima_vigencia"))
        .withColumn(
            "dw_versao_ativa",
            F.col("_proxima_vigencia").isNull() & ~F.col("dw_excluido"),
        )
        .drop("_proxima_vigencia")
    )


def _colunas_finais(df: DataFrame, settings: Settings) -> DataFrame:
    """Acrescenta rastreabilidade da carga que gerou a versão."""
    return df.withColumn("dw_batch_id", F.lit(settings.batch_id)).withColumn(
        "dw_atualizado_em", F.current_timestamp()
    )


def aplicar_scd2(
    spark: SparkSession,
    df_novo: DataFrame,
    entidade: str,
    settings: Settings,
    chave_negocio: Sequence[str],
    coluna_versao: str,
    colunas_atributos: Sequence[str],
    coluna_operacao: str | None = None,
) -> dict[str, int]:
    """Aplica SCD Tipo 2 incremental na dimensão.

    Args:
        spark: sessão ativa.
        df_novo: registros da carga, já limpos e tipados.
        entidade: nome da tabela Prata de destino.
        settings: configuração da execução.
        chave_negocio: colunas que identificam a entidade (não a versão).
        coluna_versao: timestamp da alteração na origem.
        colunas_atributos: colunas cuja mudança abre versão nova. Ficam de fora
            as colunas técnicas e as que mudam sem significado de negocio.
        coluna_operacao: coluna de operação do CDC. DELETE encerra a vigência
            sem abrir versão nova.

    Returns:
        Contadores da operação: recebidos, versões novas, ignorados por
        idempotência e ignorados por chegada atrasada.
    """
    destino = settings.tabela("silver", entidade)
    preparado = _colunas_finais(
        preparar_versoes(df_novo, chave_negocio, coluna_versao, colunas_atributos, coluna_operacao),
        settings,
    )
    recebidos = preparado.count()

    if not tabela_existe(spark, destino, settings):
        gravar_delta(preparado, destino, settings, modo="overwrite")
        logger.info(
            "dimensao criada",
            extra={
                "entidade": entidade,
                "batch_id": settings.batch_id,
                "versoes_gravadas": recebidos,
            },
        )
        return {
            "recebidos": recebidos,
            "versoes_novas": recebidos,
            "ignorados_sem_mudanca": 0,
            "ignorados_atrasados": 0,
        }

    from delta.tables import DeltaTable

    atual = ler_delta(spark, destino, settings)
    vigentes = atual.filter(F.col("dw_versao_ativa")).select(
        *[F.col(c).alias(f"_vig_{c}") for c in chave_negocio],
        F.col("dw_hash_atributos").alias("_vig_hash"),
        F.col("dw_inicio_vigencia").alias("_vig_inicio"),
    )

    condicao_join = [
        preparado[c] == vigentes[f"_vig_{c}"] for c in chave_negocio
    ]
    comparado = preparado.join(vigentes, condicao_join, "left")

    # Registro cujo conteúdo e identico ao da versão vigente: reprocessamento.
    sem_mudanca = comparado.filter(
        F.col("_vig_hash").isNotNull()
        & (F.col("_vig_hash") == F.col("dw_hash_atributos"))
        & ~F.col("dw_excluido")
    )
    ignorados_sem_mudanca = sem_mudanca.count()

    # Anterior ao vigente e chegada atrasada; igual e conflito, e aplicar
    # produziria vigência vazia. Os dois casos são descartados.
    atrasados = comparado.filter(
        F.col("_vig_inicio").isNotNull()
        & (F.col("dw_inicio_vigencia") <= F.col("_vig_inicio"))
        & (F.col("_vig_hash") != F.col("dw_hash_atributos"))
    )
    ignorados_atrasados = atrasados.count()

    aplicaveis = comparado.filter(
        F.col("_vig_hash").isNull()
        | (
            (F.col("dw_inicio_vigencia") > F.col("_vig_inicio"))
            & ((F.col("_vig_hash") != F.col("dw_hash_atributos")) | F.col("dw_excluido"))
        )
    ).drop(*[f"_vig_{c}" for c in chave_negocio], "_vig_hash", "_vig_inicio")

    versoes_novas = aplicaveis.count()

    if versoes_novas == 0:
        logger.info(
            "nenhuma versao nova, tabela inalterada",
            extra={
                "entidade": entidade,
                "batch_id": settings.batch_id,
                "recebidos": recebidos,
                "ignorados_sem_mudanca": ignorados_sem_mudanca,
                "ignorados_atrasados": ignorados_atrasados,
            },
        )
        return {
            "recebidos": recebidos,
            "versoes_novas": 0,
            "ignorados_sem_mudanca": ignorados_sem_mudanca,
            "ignorados_atrasados": ignorados_atrasados,
        }

    # Duas linhas por alteração: a de chave preenchida fecha a vigente, a de
    # chave nula entra como versão nova. Só a primeira das novas fecha, senao o
    # Delta aborta com múltiplas linhas atingindo o mesmo alvo.
    janela_fechamento = Window.partitionBy(*chave_negocio).orderBy(
        F.col("dw_inicio_vigencia").asc()
    )
    fechamento = (
        aplicaveis.withColumn("_ordem", F.row_number().over(janela_fechamento))
        .filter(F.col("_ordem") == 1)
        .drop("_ordem")
        .select(
            *[F.col(c).alias(f"_chave_merge_{c}") for c in chave_negocio],
            *aplicaveis.columns,
        )
    )
    insercao = aplicaveis.select(
        *[F.lit(None).cast(dict(aplicaveis.dtypes)[c]).alias(f"_chave_merge_{c}") for c in chave_negocio],
        *aplicaveis.columns,
    )
    staged = fechamento.unionByName(insercao)

    condicao_merge = " AND ".join(
        [f"alvo.{c} = origem._chave_merge_{c}" for c in chave_negocio]
    ) + " AND alvo.dw_versao_ativa = true"

    colunas_insert = {c: f"origem.{c}" for c in aplicaveis.columns}

    (
        DeltaTable.forName(spark, destino)
        if settings.usar_unity_catalog
        else DeltaTable.forPath(spark, destino)
    ).alias("alvo").merge(staged.alias("origem"), condicao_merge).whenMatchedUpdate(
        # A vigência anterior termina exatamente onde a nova começa.
        set={
            "dw_versao_ativa": F.lit(False),
            "dw_fim_vigencia": F.col("origem.dw_inicio_vigencia"),
            "dw_atualizado_em": F.current_timestamp(),
        }
    ).whenNotMatchedInsert(
        values=colunas_insert
    ).execute()

    logger.info(
        "scd tipo 2 aplicado",
        extra={
            "entidade": entidade,
            "batch_id": settings.batch_id,
            "recebidos": recebidos,
            "versoes_novas": versoes_novas,
            "ignorados_sem_mudanca": ignorados_sem_mudanca,
            "ignorados_atrasados": ignorados_atrasados,
        },
    )

    return {
        "recebidos": recebidos,
        "versoes_novas": versoes_novas,
        "ignorados_sem_mudanca": ignorados_sem_mudanca,
        "ignorados_atrasados": ignorados_atrasados,
    }


def versao_vigente_em(df_dimensao: DataFrame, momento: F.Column) -> DataFrame:
    """Filtra a dimensão pela versão válida em um instante.

    Usada no join temporal da Ouro, onde cada transação precisa enxergar o
    cadastro como ele estava na data em que ela ocorreu.
    """
    return df_dimensao.filter(
        (F.col("dw_inicio_vigencia") <= momento)
        & (F.col("dw_fim_vigencia").isNull() | (F.col("dw_fim_vigencia") > momento))
    )


def versao_corrente(df_dimensao: DataFrame) -> DataFrame:
    """Filtra apenas a versão corrente, para consumo que não precisa de historia."""
    return df_dimensao.filter(F.col("dw_versao_ativa") & ~F.col("dw_excluido"))
