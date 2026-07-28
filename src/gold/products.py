"""Camada Ouro: data products para análise, risco e ciência de dados.

Tres regras atravessam a camada: join temporal com a dimensão (cadastro da data
do fato), estorno abatendo o liquido, e cartão cancelado avaliado pelo status
vigente na data em vez do atual.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config.settings import Settings
from src.silver.scd import versao_corrente
from src.utils.logging import obter_logger
from src.utils.spark import gravar_delta, ler_delta

logger = obter_logger("gold")


def _juntar_dimensao_temporal(
    fato: DataFrame,
    dimensao: DataFrame,
    chave: str,
    coluna_momento: str,
    colunas: dict[str, str],
) -> DataFrame:
    """Junta o fato com a versão da dimensão vigente no momento do fato.

    O join casa a chave e exige que o momento do fato caia dentro do intervalo
    de vigência da versão. Como os intervalos de uma mesma chave não se
    sobrepõem, cada fato encontra no máximo uma versão, e o join não multiplica
    linhas.
    """
    dim = dimensao.select(
        F.col(chave).alias("_dim_chave"),
        F.col("dw_inicio_vigencia").alias("_dim_inicio"),
        F.col("dw_fim_vigencia").alias("_dim_fim"),
        *[F.col(origem).alias(destino) for origem, destino in colunas.items()],
    )

    condicao = (
        (fato[chave] == dim["_dim_chave"])
        & (dim["_dim_inicio"] <= fato[coluna_momento])
        & (dim["_dim_fim"].isNull() | (dim["_dim_fim"] > fato[coluna_momento]))
    )

    return fato.join(dim, condicao, "left").drop("_dim_chave", "_dim_inicio", "_dim_fim")


def construir_fato_transacao(spark: SparkSession, settings: Settings) -> DataFrame:
    """Fato de transação enriquecido.

    Grão: uma linha por transação. A chave e ``id_transacao`` e nada no processo
    a multiplica, o que mantém a soma de ``valor_bruto`` conciliável com a Prata.
    """
    transacoes = ler_delta(spark, settings.tabela("silver", "fato_transacao"), settings)
    estornos = ler_delta(spark, settings.tabela("silver", "fato_estorno"), settings)
    dim_cartao = ler_delta(spark, settings.tabela("silver", "dim_cartao"), settings)
    dim_conta = ler_delta(spark, settings.tabela("silver", "dim_conta"), settings)
    dim_cliente = ler_delta(spark, settings.tabela("silver", "dim_cliente"), settings)

    # Estorno parcial e multiplo: agrega antes de juntar para não multiplicar a
    # transação e inflar o valor bruto.
    estorno_por_transacao = estornos.groupBy("id_transacao").agg(
        F.sum("valor_estorno").alias("valor_estornado"),
        F.count("id_estorno").alias("qtd_estornos"),
        F.max("data_estorno").alias("data_ultimo_estorno"),
        F.first("motivo", ignorenulls=True).alias("motivo_estorno"),
    )

    base = transacoes.join(estorno_por_transacao, "id_transacao", "left").select(
        "id_transacao",
        "id_cartao",
        "data_transacao",
        "data_particao",
        F.col("valor").alias("valor_bruto"),
        F.coalesce(F.col("valor_estornado"), F.lit(0).cast("decimal(18,2)")).alias("valor_estornado"),
        F.coalesce(F.col("qtd_estornos"), F.lit(0)).alias("qtd_estornos"),
        "data_ultimo_estorno",
        "motivo_estorno",
        "mcc",
        "estabelecimento",
        "canal",
        "pais",
        "moeda",
        "dispositivo",
    )

    com_cartao = _juntar_dimensao_temporal(
        base, dim_cartao, "id_cartao", "data_transacao",
        {"id_conta": "id_conta", "tipo_cartao": "tipo_cartao",
         "status_cartao": "status_cartao_na_data", "limite": "limite_na_data"},
    )
    com_conta = _juntar_dimensao_temporal(
        com_cartao, dim_conta, "id_conta", "data_transacao",
        {"id_cliente": "id_cliente", "tipo_conta": "tipo_conta",
         "status_conta": "status_conta_na_data"},
    )
    com_cliente = _juntar_dimensao_temporal(
        com_conta, dim_cliente, "id_cliente", "data_transacao",
        {"segmento": "segmento_na_data", "cidade": "cidade_na_data",
         "estado": "estado_na_data", "renda": "renda_na_data"},
    )

    return com_cliente.select(
        "id_transacao",
        "id_cartao",
        "id_conta",
        "id_cliente",
        "data_transacao",
        "data_particao",
        F.year("data_transacao").alias("ano"),
        F.month("data_transacao").alias("mes"),
        F.date_format("data_transacao", "yyyy-MM").alias("ano_mes"),
        "valor_bruto",
        "valor_estornado",
        (F.col("valor_bruto") - F.col("valor_estornado")).alias("valor_liquido"),
        (F.col("qtd_estornos") > 0).alias("foi_estornada"),
        (F.col("valor_estornado") >= F.col("valor_bruto")).alias("estorno_total"),
        "qtd_estornos",
        "motivo_estorno",
        "mcc",
        "estabelecimento",
        "canal",
        "pais",
        "moeda",
        "dispositivo",
        "tipo_cartao",
        "status_cartao_na_data",
        "limite_na_data",
        "tipo_conta",
        "status_conta_na_data",
        "segmento_na_data",
        "cidade_na_data",
        "estado_na_data",
        "renda_na_data",
        # Compoe indicador de comportamento quando o cartão estava válido no
        # momento da compra. Cartão cancelado depois não apaga o passado.
        (
            (F.col("status_cartao_na_data") == "ATIVO")
            & (F.col("status_conta_na_data") == "ATIVA")
        ).alias("elegivel_metrica"),
    )


def construir_cliente_mes(df_fato: DataFrame, spark: SparkSession, settings: Settings) -> DataFrame:
    """Comportamento transacional por cliente e mês.

    Grão: uma linha por cliente e mês. Métricas de valor usam o liquido e apenas
    transações elegíveis; contagem de estorno usa a base total, porque estorno
    também e comportamento e some se filtrado antes.
    """
    dim_cliente = versao_corrente(
        ler_delta(spark, settings.tabela("silver", "dim_cliente"), settings)
    )

    elegiveis = df_fato.filter(F.col("elegivel_metrica"))

    agregado = elegiveis.groupBy("id_cliente", "ano_mes").agg(
        F.count("id_transacao").alias("qtd_transacoes"),
        F.sum("valor_liquido").alias("valor_liquido_total"),
        F.sum("valor_bruto").alias("valor_bruto_total"),
        F.sum("valor_estornado").alias("valor_estornado_total"),
        F.avg("valor_liquido").alias("ticket_medio"),
        F.max("valor_liquido").alias("maior_transacao"),
        F.min("valor_liquido").alias("menor_transacao"),
        F.expr("percentile_approx(valor_liquido, 0.5)").alias("mediana_transacao"),
        F.stddev("valor_liquido").alias("desvio_padrao_valor"),
        F.countDistinct("data_particao").alias("dias_com_transacao"),
        F.countDistinct("id_cartao").alias("qtd_cartoes_usados"),
        F.countDistinct("estabelecimento").alias("qtd_estabelecimentos"),
        F.countDistinct("mcc").alias("qtd_categorias_mcc"),
        F.countDistinct("canal").alias("qtd_canais"),
        F.sum(F.when(F.col("foi_estornada"), 1).otherwise(0)).alias("qtd_transacoes_estornadas"),
        F.sum(F.when(F.col("pais") != "BR", 1).otherwise(0)).alias("qtd_transacoes_exterior"),
        F.sum(F.when(F.col("canal") == "ECOMMERCE", F.col("valor_liquido")).otherwise(0)).alias("valor_ecommerce"),
        F.max("data_transacao").alias("ultima_transacao"),
        F.min("data_transacao").alias("primeira_transacao"),
    )

    return (
        agregado.join(
            dim_cliente.select("id_cliente", "segmento", "cidade", "estado", "renda"),
            "id_cliente",
            "left",
        )
        .withColumn(
            "percentual_estornado",
            F.when(
                F.col("valor_bruto_total") > 0,
                F.round(F.col("valor_estornado_total") / F.col("valor_bruto_total") * 100, 2),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "comprometimento_renda",
            F.when(
                F.col("renda") > 0,
                F.round(F.col("valor_liquido_total") / F.col("renda") * 100, 2),
            ),
        )
    )


def construir_indicadores_risco(
    df_fato: DataFrame, spark: SparkSession, settings: Settings
) -> DataFrame:
    """Visão diaria de risco: eventos, estornos e chargebacks.

    Grão: uma linha por dia. Aqui a base é a transação inteira, inclusive a não
    elegível: fraude em cartão cancelado continua sendo fraude e precisa aparecer
    no indicador.
    """
    eventos = ler_delta(spark, settings.tabela("silver", "fato_evento_risco"), settings)

    evento_por_transacao = eventos.groupBy("id_transacao").agg(
        F.count("id_evento").alias("qtd_eventos"),
        F.max(F.when(F.col("severidade") == "CRITICA", 4)
              .when(F.col("severidade") == "ALTA", 3)
              .when(F.col("severidade") == "MEDIA", 2)
              .otherwise(1)).alias("severidade_max"),
        F.max(F.when(F.col("tipo_evento") == "FRAUDE_CONFIRMADA", True).otherwise(False)).alias("tem_fraude"),
        F.max(F.when(F.col("tipo_evento") == "CHARGEBACK", True).otherwise(False)).alias("tem_chargeback"),
    )

    base = df_fato.join(evento_por_transacao, "id_transacao", "left")

    return base.groupBy("data_particao").agg(
        F.count("id_transacao").alias("qtd_transacoes"),
        F.sum("valor_bruto").alias("valor_bruto_total"),
        F.sum("valor_liquido").alias("valor_liquido_total"),
        F.sum(F.when(F.col("qtd_eventos").isNotNull(), 1).otherwise(0)).alias("qtd_com_evento_risco"),
        F.sum(F.when(F.col("tem_fraude"), 1).otherwise(0)).alias("qtd_fraude_confirmada"),
        F.sum(F.when(F.col("tem_chargeback"), 1).otherwise(0)).alias("qtd_chargeback"),
        F.sum(F.when(F.col("severidade_max") >= 3, 1).otherwise(0)).alias("qtd_severidade_alta"),
        F.sum(F.when(F.col("foi_estornada"), 1).otherwise(0)).alias("qtd_estornadas"),
        F.sum("valor_estornado").alias("valor_estornado_total"),
        F.sum(F.when(F.col("tem_fraude"), F.col("valor_bruto")).otherwise(0)).alias("valor_em_fraude"),
        F.countDistinct("id_cliente").alias("qtd_clientes_distintos"),
    ).withColumn(
        "taxa_risco_pct",
        F.round(F.col("qtd_com_evento_risco") / F.col("qtd_transacoes") * 100, 4),
    ).withColumn(
        "taxa_estorno_pct",
        F.round(F.col("qtd_estornadas") / F.col("qtd_transacoes") * 100, 4),
    ).withColumn(
        "taxa_chargeback_pct",
        F.round(F.col("qtd_chargeback") / F.col("qtd_transacoes") * 100, 4),
    )


def construir_features_cliente(
    df_cliente_mes: DataFrame, df_fato: DataFrame, spark: SparkSession, settings: Settings
) -> DataFrame:
    """Features por cliente para consumo de ciência de dados.

    Grão: uma linha por cliente, o formato que um modelo espera.

    A referência temporal e ``settings.data_referencia``, não ``current_date()``.
    Feature calculada com o relógio da máquina não e reproduzível: o mesmo
    treino rodado amanha produziria valores diferentes para o mesmo histórico, e
    o modelo em produção veria uma distribuição que nunca existiu no treino.
    """
    data_ref = F.lit(settings.data_referencia.isoformat()).cast("date")

    dim_cliente = versao_corrente(
        ler_delta(spark, settings.tabela("silver", "dim_cliente"), settings)
    )

    elegiveis = df_fato.filter(F.col("elegivel_metrica"))

    agregado = elegiveis.groupBy("id_cliente").agg(
        F.count("id_transacao").alias("f_qtd_transacoes_total"),
        F.sum("valor_liquido").alias("f_valor_liquido_total"),
        F.avg("valor_liquido").alias("f_ticket_medio"),
        F.stddev("valor_liquido").alias("f_desvio_ticket"),
        F.max("valor_liquido").alias("f_maior_transacao"),
        F.expr("percentile_approx(valor_liquido, 0.5)").alias("f_mediana_transacao"),
        F.countDistinct("data_particao").alias("f_dias_ativos"),
        F.countDistinct("id_cartao").alias("f_qtd_cartoes"),
        F.countDistinct("mcc").alias("f_qtd_categorias"),
        F.countDistinct("estabelecimento").alias("f_qtd_estabelecimentos"),
        F.max("data_particao").alias("_ultima_compra"),
        F.min("data_particao").alias("_primeira_compra"),
        F.sum(F.when(F.col("canal") == "ECOMMERCE", 1).otherwise(0)).alias("_qtd_ecommerce"),
        F.sum(F.when(F.col("pais") != "BR", 1).otherwise(0)).alias("f_qtd_exterior"),
        F.sum(F.when(F.col("foi_estornada"), 1).otherwise(0)).alias("f_qtd_estornadas"),
    )

    # Tendencia recente: comportamento dos últimos 30 dias contra o histórico.
    # E o sinal que separa cliente em ativacao de cliente em abandono, e nenhuma
    # média global captura isso.
    recentes = (
        elegiveis.filter(F.col("data_particao") >= F.date_sub(data_ref, 30))
        .groupBy("id_cliente")
        .agg(
            F.count("id_transacao").alias("f_qtd_transacoes_30d"),
            F.sum("valor_liquido").alias("f_valor_30d"),
        )
    )

    variacao_mensal = (
        df_cliente_mes.withColumn(
            "_valor_mes_anterior",
            F.lag("valor_liquido_total").over(
                Window.partitionBy("id_cliente").orderBy("ano_mes")
            ),
        )
        .withColumn(
            "_variacao",
            F.when(
                F.col("_valor_mes_anterior") > 0,
                (F.col("valor_liquido_total") - F.col("_valor_mes_anterior"))
                / F.col("_valor_mes_anterior"),
            ),
        )
        .groupBy("id_cliente")
        .agg(
            F.avg("_variacao").alias("f_variacao_media_mensal"),
            F.count("ano_mes").alias("f_qtd_meses_ativos"),
            F.avg("valor_liquido_total").alias("f_valor_medio_mensal"),
        )
    )

    return (
        dim_cliente.select("id_cliente", "segmento", "cidade", "estado", "renda")
        .join(agregado, "id_cliente", "left")
        .join(recentes, "id_cliente", "left")
        .join(variacao_mensal, "id_cliente", "left")
        .withColumn("f_recencia_dias", F.datediff(data_ref, F.col("_ultima_compra")))
        .withColumn("f_tempo_base_dias", F.datediff(data_ref, F.col("_primeira_compra")))
        .withColumn(
            "f_frequencia_mensal",
            F.when(
                F.col("f_tempo_base_dias") > 0,
                F.col("f_qtd_transacoes_total") / (F.col("f_tempo_base_dias") / 30.0),
            ),
        )
        .withColumn(
            "f_perc_ecommerce",
            F.when(
                F.col("f_qtd_transacoes_total") > 0,
                F.col("_qtd_ecommerce") / F.col("f_qtd_transacoes_total"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "f_comprometimento_renda",
            F.when(F.col("renda") > 0, F.col("f_valor_medio_mensal") / F.col("renda")),
        )
        .withColumn("f_data_referencia", data_ref)
        .drop("_ultima_compra", "_primeira_compra", "_qtd_ecommerce")
        .na.fill(
            {
                "f_qtd_transacoes_total": 0,
                "f_qtd_transacoes_30d": 0,
                "f_qtd_estornadas": 0,
                "f_qtd_exterior": 0,
                "f_qtd_meses_ativos": 0,
            }
        )
    )


def construir_dimensoes_correntes(
    spark: SparkSession, settings: Settings
) -> dict[str, DataFrame]:
    """Dimensões Ouro de cliente, conta e cartão.

    Versão corrente da Prata, sem as colunas técnicas do SCD. Respondem "como
    esta hoje"; para "como estava", use a Prata ou as colunas ``*_na_data`` do
    fato.
    """
    dim_cliente = versao_corrente(
        ler_delta(spark, settings.tabela("silver", "dim_cliente"), settings)
    ).select(
        "id_cliente", "cpf", "nome", "cidade", "estado", "renda", "segmento",
        F.col("dw_inicio_vigencia").alias("vigente_desde"),
    )

    dim_conta = versao_corrente(
        ler_delta(spark, settings.tabela("silver", "dim_conta"), settings)
    ).select(
        "id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura",
        F.col("dw_inicio_vigencia").alias("vigente_desde"),
    )

    dim_cartao = versao_corrente(
        ler_delta(spark, settings.tabela("silver", "dim_cartao"), settings)
    ).select(
        "id_cartao", "id_conta", "tipo_cartao", "limite", "status_cartao",
        F.col("dw_inicio_vigencia").alias("vigente_desde"),
    )

    return {
        "gold_dim_cliente": dim_cliente,
        "gold_dim_conta": dim_conta,
        "gold_dim_cartao": dim_cartao,
    }


def construir_dim_estabelecimento(df_fato: DataFrame) -> DataFrame:
    """Visão de estabelecimento agregada a partir do fato.

    Grão: estabelecimento e MCC. Não existe cadastro de estabelecimento na
    origem, então a dimensão e derivada do próprio movimento.
    """
    return (
        df_fato.groupBy("estabelecimento", "mcc")
        .agg(
            F.count("id_transacao").alias("qtd_transacoes"),
            F.sum("valor_liquido").alias("valor_liquido_total"),
            F.avg("valor_liquido").alias("ticket_medio"),
            F.countDistinct("id_cliente").alias("qtd_clientes_distintos"),
            F.sum(F.when(F.col("foi_estornada"), 1).otherwise(0)).alias("qtd_estornadas"),
            F.min("data_particao").alias("primeira_transacao"),
            F.max("data_particao").alias("ultima_transacao"),
        )
        .withColumn(
            "taxa_estorno_pct",
            F.round(F.col("qtd_estornadas") / F.col("qtd_transacoes") * 100, 4),
        )
    )


def materializar(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Constroi e grava todos os produtos da Ouro.

    A Ouro é reconstruída por overwrite, e não por MERGE: ela é derivada
    integralmente da Prata, não tem estado próprio e e barata de recalcular.
    Manter merge aqui adicionaria complexidade sem nada em troca.
    """
    fato = construir_fato_transacao(spark, settings).cache()
    qtd_fato = fato.count()

    gravar_delta(
        fato, settings.tabela("gold", "gold_fato_transacao"), settings,
        modo="overwrite", particoes=["data_particao"],
    )

    cliente_mes = construir_cliente_mes(fato, spark, settings)
    gravar_delta(cliente_mes, settings.tabela("gold", "gold_cliente_mes"), settings, modo="overwrite")

    risco = construir_indicadores_risco(fato, spark, settings)
    gravar_delta(risco, settings.tabela("gold", "gold_indicadores_risco"), settings, modo="overwrite")

    features = construir_features_cliente(cliente_mes, fato, spark, settings)
    gravar_delta(features, settings.tabela("gold", "gold_features_cliente"), settings, modo="overwrite")

    estabelecimento = construir_dim_estabelecimento(fato)
    gravar_delta(
        estabelecimento, settings.tabela("gold", "gold_dim_estabelecimento"), settings, modo="overwrite"
    )

    resumo = {
        "fato_transacao": qtd_fato,
        "cliente_mes": cliente_mes.count(),
        "indicadores_risco": risco.count(),
        "features_cliente": features.count(),
        "dim_estabelecimento": estabelecimento.count(),
    }

    for nome, df_dim in construir_dimensoes_correntes(spark, settings).items():
        gravar_delta(df_dim, settings.tabela("gold", nome), settings, modo="overwrite")
        resumo[nome.replace("gold_", "")] = df_dim.count()

    fato.unpersist()
    logger.info("camada ouro materializada", extra={"batch_id": settings.batch_id, **resumo})
    return resumo
