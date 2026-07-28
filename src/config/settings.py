"""Parametrizacao do pipeline.

Tudo que muda entre ambientes (paths, catalogo, data de referência, modo de
execução) entra aqui. Nenhum módulo de negocio le variável de ambiente direto.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import date


def _env(nome: str, padrao: str) -> str:
    valor = os.getenv(nome)
    return valor if valor else padrao


@dataclass(frozen=True)
class Settings:
    """Configuração de uma execução do pipeline.

    Attributes:
        ambiente: dev, hml ou prd. Define o sufixo dos schemas.
        catalogo: catalogo do Unity Catalog. Sem UC, não é usado.
        raiz_dados: diretório (ou volume) onde as camadas Delta são gravadas.
        raiz_landing: diretório onde os arquivos de origem são depositados.
        data_referencia: data lógica do processamento. Permite reprocessar o
            passado sem depender do relógio da máquina.
        batch_id: identificador único da execução, gravado em toda linha ingerida.
        modo_execucao: completa roda todas as camadas; bronze, silver e gold
            rodam apenas a camada indicada.
        usar_unity_catalog: quando True, grava em tabelas gerenciadas
            catalogo.schema.tabela. Quando False, grava em path.
    """

    ambiente: str = field(default_factory=lambda: _env("NOVAROTA_AMBIENTE", "dev"))
    catalogo: str = field(default_factory=lambda: _env("NOVAROTA_CATALOGO", "novarota"))
    raiz_dados: str = field(default_factory=lambda: _env("NOVAROTA_RAIZ_DADOS", "data/lakehouse"))
    raiz_landing: str = field(default_factory=lambda: _env("NOVAROTA_RAIZ_LANDING", "data/landing"))
    data_referencia: date = field(
        default_factory=lambda: date.fromisoformat(
            _env("NOVAROTA_DATA_REFERENCIA", date.today().isoformat())
        )
    )
    batch_id: str = field(default_factory=lambda: _env("NOVAROTA_BATCH_ID", uuid.uuid4().hex[:12]))
    modo_execucao: str = field(default_factory=lambda: _env("NOVAROTA_MODO", "completa"))
    usar_unity_catalog: bool = field(
        default_factory=lambda: _env("NOVAROTA_USAR_UC", "false").lower() == "true"
    )

    AMBIENTES = ("dev", "hml", "prd")
    MODOS = ("completa", "bronze", "silver", "gold")

    def __post_init__(self) -> None:
        if self.ambiente not in self.AMBIENTES:
            raise ValueError(
                f"ambiente invalido: {self.ambiente!r}. Esperado um de {self.AMBIENTES}"
            )
        if self.modo_execucao not in self.MODOS:
            raise ValueError(
                f"modo invalido: {self.modo_execucao!r}. Esperado um de {self.MODOS}"
            )

    def schema(self, camada: str) -> str:
        """Nome do schema da camada, com sufixo de ambiente fora de produção."""
        return camada if self.ambiente == "prd" else f"{camada}_{self.ambiente}"

    def tabela(self, camada: str, nome: str) -> str:
        """Identificador da tabela: nome qualificado no UC ou path em disco."""
        if self.usar_unity_catalog:
            return f"{self.catalogo}.{self.schema(camada)}.{nome}"
        return f"{self.raiz_dados}/{self.schema(camada)}/{nome}"

    def landing(self, origem: str) -> str:
        """Diretório de pouso dos arquivos de uma origem."""
        return f"{self.raiz_landing}/{origem}"

    def checkpoint(self, origem: str) -> str:
        """Checkpoint do Auto Loader por origem."""
        return f"{self.raiz_dados}/_checkpoints/{self.schema('bronze')}/{origem}"


def settings_de_teste(raiz: str, data_referencia: date | None = None) -> Settings:
    """Settings apontando para um diretório temporario, usado pelos testes."""
    return Settings(
        ambiente="dev",
        catalogo="novarota",
        raiz_dados=f"{raiz}/lakehouse",
        raiz_landing=f"{raiz}/landing",
        data_referencia=data_referencia or date(2026, 3, 31),
        batch_id="teste0000",
        modo_execucao="completa",
        usar_unity_catalog=False,
    )
