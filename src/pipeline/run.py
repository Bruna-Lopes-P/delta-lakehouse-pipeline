"""Orquestração do pipeline.

Cada etapa é medida e o resultado vai para a tabela de observabilidade. Falha
interrompe a execução em vez de seguir com carga parcial: uma Ouro construída
sobre uma Prata incompleta produz número errado sem sinalizar nada, o que é pior
do que não produzir número nenhum.
"""

from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import SparkSession

from src.config.settings import Settings
from src.gold.products import materializar
from src.ingestion.bronze import ingerir
from src.observability.metrics import ColetorMetricas
from src.silver.dimensions import processar_cartao, processar_cliente, processar_conta
from src.silver.facts import processar_estorno, processar_evento_risco, processar_transacao
from src.utils.logging import configurar_logs, obter_logger
from src.utils.spark import obter_sessao, otimizar

logger = obter_logger("pipeline")

ORIGENS = [
    ("clientes", "clientes", "json"),
    ("contas", "contas", "csv"),
    ("cartoes", "cartoes", "csv"),
    ("transacoes", "transacoes", "csv"),
    ("estornos", "estornos", "csv"),
    ("eventos_risco", "eventos_risco", "csv"),
]


def executar_bronze(spark: SparkSession, settings: Settings, coletor: ColetorMetricas) -> None:
    """Ingere todas as origens na Bronze."""
    for entidade, origem, formato in ORIGENS:
        with coletor.medir("bronze", "ingestao", entidade) as medicao:
            df = ingerir(spark, entidade, origem, settings, formato=formato)
            if df is not None:
                quantidade = df.count()
                medicao.linhas_lidas = quantidade
                medicao.linhas_gravadas = quantidade


def executar_silver(spark: SparkSession, settings: Settings, coletor: ColetorMetricas) -> None:
    """Processa a Prata na ordem de dependencia.

    A ordem importa: conta válida contra cliente, cartão contra conta, transação
    contra cartão, estorno e evento contra transação. Inverter faria a validação
    de integridade rodar contra uma tabela ainda vazia.
    """
    etapas = [
        ("dim_cliente", processar_cliente),
        ("dim_conta", processar_conta),
        ("dim_cartao", processar_cartao),
        ("fato_transacao", processar_transacao),
        ("fato_estorno", processar_estorno),
        ("fato_evento_risco", processar_evento_risco),
    ]

    for entidade, funcao in etapas:
        with coletor.medir("silver", "transformacao", entidade) as medicao:
            resultado = funcao(spark, settings)
            medicao.linhas_lidas = resultado.get("lidos", 0)
            medicao.linhas_rejeitadas = resultado.get("rejeitados", 0)
            medicao.linhas_gravadas = resultado.get(
                "gravados", resultado.get("versoes_novas", 0)
            )

    # Compactação ao fim da camada. Cada MERGE gera arquivo novo; sem compactar,
    # o número de arquivos cresce a cada execução e a leitura degrada.
    otimizar(spark, settings.tabela("silver", "fato_transacao"), settings,
             zorder=["id_cartao", "id_transacao"])
    otimizar(spark, settings.tabela("silver", "dim_cliente"), settings, zorder=["id_cliente"])


def executar_gold(spark: SparkSession, settings: Settings, coletor: ColetorMetricas) -> None:
    """Materializa os produtos da Ouro."""
    with coletor.medir("gold", "materializacao", "todos") as medicao:
        resumo = materializar(spark, settings)
        medicao.linhas_gravadas = sum(resumo.values())

    otimizar(spark, settings.tabela("gold", "gold_fato_transacao"), settings,
             zorder=["id_cliente", "data_transacao"])


def executar(settings: Settings, spark: SparkSession = None) -> dict[str, int]:
    """Roda o pipeline no modo configurado.

    Returns:
        Resumo das métricas coletadas.
    """
    spark = spark or obter_sessao()
    coletor = ColetorMetricas(spark, settings)

    logger.info(
        "pipeline iniciado",
        extra={
            "batch_id": settings.batch_id,
            "ambiente": settings.ambiente,
            "modo": settings.modo_execucao,
            "data_referencia": settings.data_referencia.isoformat(),
        },
    )

    try:
        if settings.modo_execucao in ("completa", "bronze"):
            executar_bronze(spark, settings, coletor)
        if settings.modo_execucao in ("completa", "silver"):
            executar_silver(spark, settings, coletor)
        if settings.modo_execucao in ("completa", "gold"):
            executar_gold(spark, settings, coletor)
    finally:
        # Persiste o que foi medido até aqui mesmo quando a execução falha: a
        # métrica da etapa que quebrou e justamente a que interessa investigar.
        coletor.persistir()

    resumo = coletor.resumo()
    logger.info("pipeline concluido", extra={"batch_id": settings.batch_id, **resumo})
    return resumo


def main() -> None:
    """Entrada de linha de comando."""
    parser = argparse.ArgumentParser(description="Pipeline Lakehouse NovaRota")
    parser.add_argument("--ambiente", default=None, choices=["dev", "hml", "prd"])
    parser.add_argument("--modo", default=None, choices=["completa", "bronze", "silver", "gold"])
    parser.add_argument("--data-referencia", default=None, help="AAAA-MM-DD")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--gerar-massa", action="store_true", help="gera dados sinteticos antes")
    parser.add_argument("--log", default="INFO")
    args = parser.parse_args()

    configurar_logs(args.log)

    campos = {}
    if args.ambiente:
        campos["ambiente"] = args.ambiente
    if args.modo:
        campos["modo_execucao"] = args.modo
    if args.data_referencia:
        campos["data_referencia"] = date.fromisoformat(args.data_referencia)
    if args.batch_id:
        campos["batch_id"] = args.batch_id

    settings = Settings(**campos)

    if args.gerar_massa:
        from src.datagen.generator import gerar_massa

        gerar_massa(settings)

    executar(settings)


if __name__ == "__main__":
    main()
