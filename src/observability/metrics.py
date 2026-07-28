"""Métricas de execução do pipeline.

Cada etapa registra linhas lidas, gravadas, rejeitadas e duração em tabela
Delta, não só no log: a pergunta operacional e sobre série historica.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config.settings import Settings
from src.utils.logging import obter_logger
from src.utils.spark import gravar_delta

logger = obter_logger("observabilidade")

ESQUEMA_METRICAS = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("data_referencia", StringType(), False),
        StructField("camada", StringType(), False),
        StructField("etapa", StringType(), False),
        StructField("entidade", StringType(), False),
        StructField("linhas_lidas", LongType(), True),
        StructField("linhas_gravadas", LongType(), True),
        StructField("linhas_rejeitadas", LongType(), True),
        StructField("duracao_segundos", IntegerType(), True),
        StructField("status", StringType(), False),
        StructField("detalhe_erro", StringType(), True),
        StructField("iniciado_em", TimestampType(), False),
        StructField("finalizado_em", TimestampType(), False),
    ]
)


@dataclass
class Medicao:
    """Uma etapa medida."""

    camada: str
    etapa: str
    entidade: str
    linhas_lidas: int = 0
    linhas_gravadas: int = 0
    linhas_rejeitadas: int = 0
    status: str = "SUCESSO"
    detalhe_erro: str | None = None
    iniciado_em: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finalizado_em: datetime | None = None
    _inicio_monotonico: float = field(default_factory=time.monotonic, repr=False)

    def encerrar(self) -> None:
        if self.finalizado_em is None:
            self.finalizado_em = datetime.now(timezone.utc)

    @property
    def duracao_segundos(self) -> int:
        return int(time.monotonic() - self._inicio_monotonico)


class ColetorMetricas:
    """Acumula medicoes durante a execução e persiste ao final.

    Uso:
        coletor = ColetorMetricas(spark, settings)
        with coletor.medir("bronze", "ingestão", "clientes") as m:
            m.linhas_lidas = df.count()
            ...
        coletor.persistir()
    """

    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings
        self.medicoes: list[Medicao] = []

    def medir(self, camada: str, etapa: str, entidade: str) -> _ContextoMedicao:
        return _ContextoMedicao(self, Medicao(camada=camada, etapa=etapa, entidade=entidade))

    def registrar(self, medicao: Medicao) -> None:
        medicao.encerrar()
        self.medicoes.append(medicao)
        logger.info(
            "etapa concluida",
            extra={
                "batch_id": self.settings.batch_id,
                "camada": medicao.camada,
                "etapa": medicao.etapa,
                "entidade": medicao.entidade,
                "linhas_lidas": medicao.linhas_lidas,
                "linhas_gravadas": medicao.linhas_gravadas,
                "linhas_rejeitadas": medicao.linhas_rejeitadas,
                "duracao_segundos": medicao.duracao_segundos,
                "status": medicao.status,
            },
        )

    def resumo(self) -> dict[str, int]:
        """Totais da execução, usado no fim do pipeline e nos testes."""
        return {
            "etapas": len(self.medicoes),
            "linhas_lidas": sum(m.linhas_lidas for m in self.medicoes),
            "linhas_gravadas": sum(m.linhas_gravadas for m in self.medicoes),
            "linhas_rejeitadas": sum(m.linhas_rejeitadas for m in self.medicoes),
            "etapas_com_falha": sum(1 for m in self.medicoes if m.status == "FALHA"),
        }

    def persistir(self) -> None:
        """Grava as medicoes na tabela de observabilidade."""
        if not self.medicoes:
            return

        linhas = [
            (
                self.settings.batch_id,
                self.settings.data_referencia.isoformat(),
                m.camada,
                m.etapa,
                m.entidade,
                m.linhas_lidas,
                m.linhas_gravadas,
                m.linhas_rejeitadas,
                m.duracao_segundos,
                m.status,
                m.detalhe_erro,
                m.iniciado_em,
                m.finalizado_em,
            )
            for m in self.medicoes
        ]

        df = self.spark.createDataFrame(linhas, schema=ESQUEMA_METRICAS)
        destino = self.settings.tabela("observabilidade", "execucao_pipeline")
        gravar_delta(df, destino, self.settings, modo="append")

        logger.info(
            "metricas persistidas",
            extra={"batch_id": self.settings.batch_id, "destino": destino, **self.resumo()},
        )


class _ContextoMedicao:
    """Context manager que fecha a medicao mesmo quando a etapa falha.

    A exceção não e engolida: e registrada com status FALHA e relancada. Mascarar
    falha critica esconderia carga incompleta.
    """

    def __init__(self, coletor: ColetorMetricas, medicao: Medicao):
        self.coletor = coletor
        self.medicao = medicao

    def __enter__(self) -> Medicao:
        logger.info(
            "etapa iniciada",
            extra={
                "batch_id": self.coletor.settings.batch_id,
                "camada": self.medicao.camada,
                "etapa": self.medicao.etapa,
                "entidade": self.medicao.entidade,
            },
        )
        return self.medicao

    def __exit__(self, tipo_exc, valor_exc, traceback) -> bool:
        if tipo_exc is not None:
            self.medicao.status = "FALHA"
            self.medicao.detalhe_erro = f"{tipo_exc.__name__}: {valor_exc}"
        self.coletor.registrar(self.medicao)
        return False  # nao suprime a excecao
