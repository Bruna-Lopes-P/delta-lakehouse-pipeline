"""Regras de qualidade declarativas.

Regra é um predicado nomeado do que é válido. Como dado, e não como cadeia de
``.filter()``, da para apontar qual regra rejeitou cada linha e testar a regra
isolada.

REJEITA manda para quarentena; ALERTA só contabiliza.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

REJEITA = "REJEITA"
ALERTA = "ALERTA"

UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
]


@dataclass(frozen=True)
class Regra:
    """Regra de qualidade.

    Attributes:
        nome: identificador curto, gravado na quarentena.
        descricao: o que a regra garante, em linguagem de negocio.
        condição: expressão que resulta True quando a linha e válida.
        severidade: REJEITA manda para quarentena, ALERTA apenas contabiliza.
    """

    nome: str
    descricao: str
    condicao: Column
    severidade: str = REJEITA


def aplicar_regras(df: DataFrame, regras: Sequence[Regra]) -> tuple[DataFrame, DataFrame]:
    """Separa o DataFrame em aprovados e rejeitados.

    Nulo na condição conta como violacao: sem o coalesce, a comparação devolve
    NULL e a linha escapa da regra.

    Args:
        df: dados a validar.
        regras: regras a aplicar, na ordem em que devem ser avaliadas.

    Returns:
        (aprovados, rejeitados). ``rejeitados`` ganha a coluna
        ``motivos_rejeicao``, um array com o nome de cada regra violada.
    """
    if not regras:
        vazio = df.limit(0).withColumn("motivos_rejeicao", F.array().cast("array<string>"))
        return df, vazio

    bloqueantes = [r for r in regras if r.severidade == REJEITA]
    if not bloqueantes:
        vazio = df.limit(0).withColumn("motivos_rejeicao", F.array().cast("array<string>"))
        return df, vazio

    motivos = F.array_compact(
        F.array(
            *[
                F.when(~F.coalesce(regra.condicao, F.lit(False)), F.lit(regra.nome))
                for regra in bloqueantes
            ]
        )
    )

    marcado = df.withColumn("motivos_rejeicao", motivos)

    aprovados = marcado.filter(F.size("motivos_rejeicao") == 0).drop("motivos_rejeicao")
    rejeitados = marcado.filter(F.size("motivos_rejeicao") > 0)

    return aprovados, rejeitados


def regras_cliente() -> list[Regra]:
    """Regras da entidade cliente."""
    return [
        Regra(
            nome="cliente_id_obrigatorio",
            descricao="id_cliente e a chave de negócio e não pode ser nulo",
            condicao=F.col("id_cliente").isNotNull(),
        ),
        Regra(
            nome="cliente_cpf_obrigatorio",
            descricao="cliente sem CPF não pode ser identificado",
            condicao=F.col("cpf").isNotNull(),
        ),
        Regra(
            nome="cliente_cpf_11_digitos",
            descricao="CPF precisa ter 11 digitos numericos",
            condicao=F.col("cpf").rlike(r"^\d{11}$"),
        ),
        Regra(
            nome="cliente_renda_nao_negativa",
            descricao="renda negativa indica erro de origem",
            condicao=F.col("renda").isNull() | (F.col("renda") >= 0),
        ),
        Regra(
            nome="cliente_uf_valida",
            descricao="estado precisa ser uma UF brasileira",
            condicao=F.col("estado").isin(UFS_VALIDAS),
        ),
        Regra(
            nome="cliente_data_atualizacao_obrigatoria",
            descricao="sem data_atualizacao não da para ordenar as versões do SCD",
            condicao=F.col("data_atualizacao").isNotNull(),
        ),
    ]


def regras_conta() -> list[Regra]:
    """Regras da entidade conta."""
    return [
        Regra(
            nome="conta_id_obrigatorio",
            descricao="id_conta e a chave de negócio e não pode ser nulo",
            condicao=F.col("id_conta").isNotNull(),
        ),
        Regra(
            nome="conta_cliente_obrigatorio",
            descricao="conta precisa apontar para um cliente",
            condicao=F.col("id_cliente").isNotNull(),
        ),
        Regra(
            nome="conta_status_conhecido",
            descricao="status fora do domínio impede classificar a conta",
            condicao=F.col("status_conta").isin("ATIVA", "ENCERRADA", "SUSPENSA", "BLOQUEADA"),
        ),
        Regra(
            nome="conta_abertura_nao_futura",
            descricao="data de abertura no futuro indica erro de origem",
            condicao=F.col("data_abertura") <= F.current_date(),
        ),
    ]


def regras_cartao() -> list[Regra]:
    """Regras da entidade cartão."""
    return [
        Regra(
            nome="cartao_id_obrigatorio",
            descricao="id_cartao e a chave de negócio e não pode ser nulo",
            condicao=F.col("id_cartao").isNotNull(),
        ),
        Regra(
            nome="cartao_conta_obrigatoria",
            descricao="cartão precisa apontar para uma conta",
            condicao=F.col("id_conta").isNotNull(),
        ),
        Regra(
            nome="cartao_status_conhecido",
            descricao="status fora do domínio impede aplicar a regra de cancelamento",
            condicao=F.col("status_cartao").isin("ATIVO", "CANCELADO", "BLOQUEADO"),
        ),
        Regra(
            nome="cartao_limite_nao_negativo",
            descricao="limite negativo indica erro de origem",
            condicao=F.col("limite").isNull() | (F.col("limite") >= 0),
        ),
    ]


def regras_transacao() -> list[Regra]:
    """Regras da entidade transação."""
    return [
        Regra(
            nome="transacao_id_obrigatorio",
            descricao="id_transacao e a chave de deduplicacao e não pode ser nulo",
            condicao=F.col("id_transacao").isNotNull(),
        ),
        Regra(
            nome="transacao_cartao_obrigatorio",
            descricao="transação precisa apontar para um cartão",
            condicao=F.col("id_cartao").isNotNull(),
        ),
        Regra(
            nome="transacao_valor_positivo",
            descricao="valor zero ou negativo não representa uma compra",
            condicao=F.col("valor") > 0,
        ),
        Regra(
            nome="transacao_data_obrigatoria",
            descricao="sem data não da para particionar nem apurar o mes",
            condicao=F.col("data_transacao").isNotNull(),
        ),
        Regra(
            nome="transacao_data_nao_futura",
            descricao="transação com data futura indica erro de origem",
            condicao=F.to_date("data_transacao") <= F.current_date(),
        ),
        Regra(
            nome="transacao_moeda_conhecida",
            descricao="moeda fora do domínio impede converter o valor",
            condicao=F.col("moeda").isin("BRL", "USD", "EUR"),
        ),
    ]


def regras_estorno() -> list[Regra]:
    """Regras da entidade estorno."""
    return [
        Regra(
            nome="estorno_id_obrigatorio",
            descricao="id_estorno e a chave e não pode ser nulo",
            condicao=F.col("id_estorno").isNotNull(),
        ),
        Regra(
            nome="estorno_transacao_obrigatoria",
            descricao="estorno precisa apontar para uma transação",
            condicao=F.col("id_transacao").isNotNull(),
        ),
        Regra(
            nome="estorno_valor_positivo",
            descricao="valor de estorno precisa ser maior que zero",
            condicao=F.col("valor_estorno") > 0,
        ),
    ]


def regras_evento_risco() -> list[Regra]:
    """Regras da entidade evento de risco."""
    return [
        Regra(
            nome="evento_id_obrigatorio",
            descricao="id_evento e a chave e não pode ser nulo",
            condicao=F.col("id_evento").isNotNull(),
        ),
        Regra(
            nome="evento_transacao_obrigatoria",
            descricao="evento precisa apontar para uma transação",
            condicao=F.col("id_transacao").isNotNull(),
        ),
        Regra(
            nome="evento_severidade_conhecida",
            descricao="severidade fora do domínio impede priorizar a análise",
            condicao=F.col("severidade").isin("BAIXA", "MEDIA", "ALTA", "CRITICA"),
        ),
    ]
