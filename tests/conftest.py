"""Fixtures da suíte.

A sessão Spark e criada uma vez por sessão de teste, porque subir a JVM custa
alguns segundos e repetir isso por teste tornaria a suíte inutilizavel no dia a
dia. Ja o diretório de dados e por teste: cada caso começa com um lakehouse
vazio, então a ordem de execução não muda o resultado.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from collections.abc import Iterator
from datetime import date, datetime

import pytest

# O worker do Spark sobe um interpretador Python separado do driver. Sem apontar
# explicitamente, ele pega o primeiro python do PATH, que pode ser uma versão
# incompatível com a do ambiente virtual. O sintoma é um erro de socket no meio
# da execução, que não parece ter relacao com versão de Python.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# No Windows o Hadoop precisa do winutils e do hadoop.dll para operar no sistema
# de arquivos local. Sem isso, a escrita falha com UnsatisfiedLinkError em
# NativeIO$Windows.access0.
#
# O PATH precisa usar barra invertida: o carregador de DLL do Windows não
# resolve entrada de PATH escrita com barra normal, e o sintoma é a JVM se
# comportar como se a biblioteca não existisse.
if sys.platform == "win32":
    _hadoop_home = os.environ.get("HADOOP_HOME") or r"C:\hadoop"
    _hadoop_bin = os.path.join(_hadoop_home, "bin")

    if os.path.exists(os.path.join(_hadoop_bin, "winutils.exe")):
        os.environ["HADOOP_HOME"] = _hadoop_home
        if _hadoop_bin.lower() not in os.environ.get("PATH", "").lower():
            os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    else:
        raise RuntimeError(
            f"winutils.exe nao encontrado em {_hadoop_bin}. "
            "Veja a secao de execucao local no README."
        )

from pyspark.sql import SparkSession  # noqa: E402

from src.config.settings import Settings, settings_de_teste  # noqa: E402

DATA_REFERENCIA = date(2026, 3, 31)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Sessão Spark local com Delta, compartilhada pela suíte."""
    from delta import configure_spark_with_delta_pip

    construtor = (
        SparkSession.builder.appName("novarota-testes")
        .master("local[2]")
        # A sessão é compartilhada pela suíte inteira e o driver acumula
        # metadados de todos os stages executados. Com o heap padrão de 1 GB, a
        # suíte completa estoura em OutOfMemoryError depois de alguns milhares
        # de stages, e o sintoma é uma cascata de ConnectionRefusedError nos
        # testes seguintes, que não tem relacao aparente com memória.
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="novarota-wh-"))
        # Mesma configuração de obter_sessao, para que o teste rode na mesma
        # condição da produção.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # ANSI ligado e o padrao do Spark 4 e do Databricks Runtime recente, e e
        # a configuracao mais rigorosa: cast invalido lanca excecao em vez de
        # devolver nulo em silencio. Testar assim garante que a Prata converte
        # com try_cast e manda o dado ruim para a quarentena, em vez de quebrar.
        .config("spark.sql.ansi.enabled", "true")
    )

    sessao = configure_spark_with_delta_pip(construtor).getOrCreate()
    sessao.sparkContext.setLogLevel("ERROR")

    yield sessao

    sessao.stop()


@pytest.fixture
def raiz_temporaria() -> Iterator[str]:
    """Diretório isolado por teste."""
    caminho = tempfile.mkdtemp(prefix="novarota-teste-")
    yield caminho.replace("\\", "/")
    shutil.rmtree(caminho, ignore_errors=True)


@pytest.fixture
def settings(raiz_temporaria: str) -> Settings:
    """Settings apontando para o diretório isolado do teste."""
    return settings_de_teste(raiz_temporaria, DATA_REFERENCIA)


def ts(texto: str) -> datetime:
    """Atalho para escrever timestamp nos casos de teste."""
    return datetime.fromisoformat(texto)
