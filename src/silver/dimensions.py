"""Dimensões da Prata: cliente, conta e cartão.

Mesma sequência para as três: tipagem, qualidade, integridade referencial e
SCD Tipo 2. Muda só o conjunto de atributos que dispara versão nova.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.settings import Settings
from src.quality.quarantine import gravar_quarentena, separar_orfaos
from src.quality.rules import (
    aplicar_regras,
    regras_cartao,
    regras_cliente,
    regras_conta,
)
from src.silver.scd import aplicar_scd2, versao_corrente
from src.silver.tipos import converter, texto, texto_padronizado
from src.utils.logging import obter_logger
from src.utils.spark import ler_delta

logger = obter_logger("silver.dimensoes")


def tipar_cliente(df_bronze: DataFrame) -> DataFrame:
    """Converte o cliente bruto para os tipos do modelo.

    O CPF continua string: e identificador, não número. Convertido para inteiro
    perderia o zero a esquerda e passaria a aceitar operação aritmetica que não
    faz sentido.
    """
    return df_bronze.select(
        converter("id_cliente", "bigint").alias("id_cliente"),
        texto("cpf").alias("cpf"),
        texto("nome").alias("nome"),
        texto("cidade").alias("cidade"),
        texto_padronizado("estado").alias("estado"),
        converter("renda", "decimal(18,2)").alias("renda"),
        texto_padronizado("segmento").alias("segmento"),
        converter("data_atualizacao", "timestamp").alias("data_atualizacao"),
        texto_padronizado("operacao").alias("operacao"),
        F.col("arquivo_origem"),
        F.col("batch_id"),
    )


def tipar_conta(df_bronze: DataFrame) -> DataFrame:
    """Converte a conta bruta para os tipos do modelo."""
    return df_bronze.select(
        converter("id_conta", "bigint").alias("id_conta"),
        converter("id_cliente", "bigint").alias("id_cliente"),
        texto_padronizado("tipo_conta").alias("tipo_conta"),
        texto_padronizado("status_conta").alias("status_conta"),
        converter("data_abertura", "date").alias("data_abertura"),
        converter("data_atualizacao", "timestamp").alias("data_atualizacao"),
        texto_padronizado("operacao").alias("operacao"),
        F.col("arquivo_origem"),
        F.col("batch_id"),
    )


def tipar_cartao(df_bronze: DataFrame) -> DataFrame:
    """Converte o cartão bruto para os tipos do modelo."""
    return df_bronze.select(
        converter("id_cartao", "bigint").alias("id_cartao"),
        converter("id_conta", "bigint").alias("id_conta"),
        texto_padronizado("tipo_cartao").alias("tipo_cartao"),
        converter("limite", "decimal(18,2)").alias("limite"),
        texto_padronizado("status_cartao").alias("status_cartao"),
        converter("data_atualizacao", "timestamp").alias("data_atualizacao"),
        texto_padronizado("operacao").alias("operacao"),
        F.col("arquivo_origem"),
        F.col("batch_id"),
    )


def _validar(
    df: DataFrame,
    regras,
    entidade: str,
    settings: Settings,
) -> tuple[DataFrame, int]:
    """Aplica regras e envia rejeitados para quarentena."""
    aprovados, rejeitados = aplicar_regras(df, regras)
    quantidade = gravar_quarentena(rejeitados, entidade, settings)
    return aprovados, quantidade


def processar_cliente(
    spark: SparkSession, settings: Settings
) -> dict[str, int]:
    """Bronze de cliente para dimensão Prata versionada."""
    bronze = ler_delta(spark, settings.tabela("bronze", "clientes"), settings)
    tipado = tipar_cliente(bronze)
    lidos = tipado.count()

    aprovados, rejeitados = _validar(tipado, regras_cliente(), "cliente", settings)

    resultado = aplicar_scd2(
        spark=spark,
        df_novo=aprovados,
        entidade="dim_cliente",
        settings=settings,
        chave_negocio=["id_cliente"],
        coluna_versao="data_atualizacao",
        colunas_atributos=["nome", "cidade", "estado", "renda", "segmento"],
        coluna_operacao="operacao",
    )

    return {"lidos": lidos, "rejeitados": rejeitados, **resultado}


def processar_conta(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Bronze de conta para dimensão Prata versionada.

    Uma conta que aponta para cliente inexistente vai para quarentena: sem o
    cliente, não ha como atribuir a transação ao titular no fato.
    """
    bronze = ler_delta(spark, settings.tabela("bronze", "contas"), settings)
    tipado = tipar_conta(bronze)
    lidos = tipado.count()

    aprovados, rejeitados_regra = _validar(tipado, regras_conta(), "conta", settings)

    dim_cliente = versao_corrente(
        ler_delta(spark, settings.tabela("silver", "dim_cliente"), settings)
    )
    integros, orfaos = separar_orfaos(
        df_filho=aprovados,
        df_pai=dim_cliente,
        chave_estrangeira="id_cliente",
        chave_pai="id_cliente",
        nome_motivo="conta_sem_cliente_valido",
    )
    rejeitados_orfao = gravar_quarentena(orfaos, "conta", settings)

    resultado = aplicar_scd2(
        spark=spark,
        df_novo=integros,
        entidade="dim_conta",
        settings=settings,
        chave_negocio=["id_conta"],
        coluna_versao="data_atualizacao",
        colunas_atributos=["id_cliente", "tipo_conta", "status_conta", "data_abertura"],
        coluna_operacao="operacao",
    )

    return {
        "lidos": lidos,
        "rejeitados": rejeitados_regra + rejeitados_orfao,
        "rejeitados_orfaos": rejeitados_orfao,
        **resultado,
    }


def processar_cartao(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Bronze de cartão para dimensão Prata versionada.

    Cartão sem conta válida vai para quarentena pelo mesmo motivo da conta sem
    cliente: quebra a cadeia cartão -> conta -> cliente que o fato precisa.
    """
    bronze = ler_delta(spark, settings.tabela("bronze", "cartoes"), settings)
    tipado = tipar_cartao(bronze)
    lidos = tipado.count()

    aprovados, rejeitados_regra = _validar(tipado, regras_cartao(), "cartao", settings)

    dim_conta = versao_corrente(
        ler_delta(spark, settings.tabela("silver", "dim_conta"), settings)
    )
    integros, orfaos = separar_orfaos(
        df_filho=aprovados,
        df_pai=dim_conta,
        chave_estrangeira="id_conta",
        chave_pai="id_conta",
        nome_motivo="cartao_sem_conta_valida",
    )
    rejeitados_orfao = gravar_quarentena(orfaos, "cartao", settings)

    resultado = aplicar_scd2(
        spark=spark,
        df_novo=integros,
        entidade="dim_cartao",
        settings=settings,
        chave_negocio=["id_cartao"],
        coluna_versao="data_atualizacao",
        colunas_atributos=["id_conta", "tipo_cartao", "limite", "status_cartao"],
        coluna_operacao="operacao",
    )

    return {
        "lidos": lidos,
        "rejeitados": rejeitados_regra + rejeitados_orfao,
        "rejeitados_orfaos": rejeitados_orfao,
        **resultado,
    }
