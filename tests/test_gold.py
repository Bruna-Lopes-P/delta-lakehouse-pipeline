"""Testes das regras de negocio da camada Ouro.

Cobrem os cenários criticos do domínio: transação estornada fora do valor
liquido, cartão cancelado fora da métrica mas presente no histórico, e cadastro
lido na versão vigente na data da transação.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from src.gold.products import (
    construir_cliente_mes,
    construir_fato_transacao,
    construir_indicadores_risco,
)
from src.utils.spark import gravar_delta


def ts(texto: str) -> datetime:
    return datetime.fromisoformat(texto)


def _montar_prata(spark, settings, transacoes, estornos, cartoes, contas, clientes, eventos=None):
    """Grava as tabelas Prata que a Ouro consome."""
    gravar_delta(transacoes, settings.tabela("silver", "fato_transacao"), settings, modo="overwrite")
    gravar_delta(estornos, settings.tabela("silver", "fato_estorno"), settings, modo="overwrite")
    gravar_delta(cartoes, settings.tabela("silver", "dim_cartao"), settings, modo="overwrite")
    gravar_delta(contas, settings.tabela("silver", "dim_conta"), settings, modo="overwrite")
    gravar_delta(clientes, settings.tabela("silver", "dim_cliente"), settings, modo="overwrite")

    if eventos is None:
        eventos = spark.createDataFrame(
            [], "id_evento long, id_transacao long, tipo_evento string, severidade string, data_evento date"
        )
    gravar_delta(eventos, settings.tabela("silver", "fato_evento_risco"), settings, modo="overwrite")


# Os schemas são declarados: colunas como dw_fim_vigencia ficam nulas em todas as
# linhas de alguns casos, e o Spark não consegue inferir tipo a partir só de nulo.
ESQUEMA_TRANSACAO = (
    "id_transacao long, id_cartao long, data_transacao timestamp, valor decimal(18,2), "
    "mcc int, estabelecimento string, canal string, pais string, moeda string, "
    "dispositivo string, data_particao date"
)
ESQUEMA_ESTORNO = (
    "id_estorno long, id_transacao long, data_estorno date, "
    "valor_estorno decimal(18,2), motivo string"
)
ESQUEMA_CARTAO = (
    "id_cartao long, id_conta long, tipo_cartao string, limite decimal(18,2), "
    "status_cartao string, dw_inicio_vigencia timestamp, dw_fim_vigencia timestamp, "
    "dw_versao_ativa boolean, dw_excluido boolean"
)
ESQUEMA_CONTA = (
    "id_conta long, id_cliente long, tipo_conta string, status_conta string, "
    "dw_inicio_vigencia timestamp, dw_fim_vigencia timestamp, "
    "dw_versao_ativa boolean, dw_excluido boolean"
)
ESQUEMA_CLIENTE = (
    "id_cliente long, nome string, cidade string, estado string, segmento string, "
    "renda decimal(18,2), dw_inicio_vigencia timestamp, dw_fim_vigencia timestamp, "
    "dw_versao_ativa boolean, dw_excluido boolean"
)
ESQUEMA_EVENTO = (
    "id_evento long, id_transacao long, tipo_evento string, severidade string, "
    "data_evento date"
)


def _transacoes(spark, linhas):
    return spark.createDataFrame(linhas, ESQUEMA_TRANSACAO)


def _estornos(spark, linhas):
    return spark.createDataFrame(linhas, ESQUEMA_ESTORNO)


def _estornos_vazio(spark):
    return spark.createDataFrame([], ESQUEMA_ESTORNO)


def _cartoes(spark, linhas):
    return spark.createDataFrame(linhas, ESQUEMA_CARTAO)


def _contas(spark, linhas):
    return spark.createDataFrame(linhas, ESQUEMA_CONTA)


def _clientes(spark, linhas):
    return spark.createDataFrame(linhas, ESQUEMA_CLIENTE)


CARTAO_ATIVO = [(10, 100, "CREDITO", Decimal("5000.00"), "ATIVO", ts("2025-01-01 00:00:00"), None, True, False)]
CONTA_ATIVA = [(100, 1, "CORRENTE", "ATIVA", ts("2025-01-01 00:00:00"), None, True, False)]
CLIENTE_BASE = [(1, "Ana", "Porto Alegre", "RS", "VAREJO", Decimal("5000.00"),
                 ts("2025-01-01 00:00:00"), None, True, False)]


class TestEstorno:
    def test_estorno_total_zera_o_valor_liquido(self, spark, settings):
        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
            ]),
            _estornos(spark, [(900, 1, date(2026, 3, 12), Decimal('500.00'), "NAO_RECONHECIDO")]),
            _cartoes(spark, CARTAO_ATIVO), _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
        )

        linha = construir_fato_transacao(spark, settings).collect()[0]

        assert float(linha["valor_bruto"]) == 500.0
        assert float(linha["valor_estornado"]) == 500.0
        assert float(linha["valor_liquido"]) == 0.0
        assert linha["foi_estornada"] is True
        assert linha["estorno_total"] is True

    def test_estorno_parcial_abate_apenas_a_parte_estornada(self, spark, settings):
        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
            ]),
            _estornos(spark, [(900, 1, date(2026, 3, 12), Decimal('200.00'), "CANCELAMENTO_COMPRA")]),
            _cartoes(spark, CARTAO_ATIVO), _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
        )

        linha = construir_fato_transacao(spark, settings).collect()[0]

        assert float(linha["valor_liquido"]) == 300.0
        assert linha["foi_estornada"] is True
        assert linha["estorno_total"] is False

    def test_dois_estornos_parciais_nao_multiplicam_a_transacao(self, spark, settings):
        """Sem agregar antes do join, a transação viraria duas linhas e o valor
        bruto dobraria."""
        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
            ]),
            _estornos(spark, [
                (900, 1, date(2026, 3, 12), Decimal('200.00'), "CANCELAMENTO_COMPRA"),
                (901, 1, date(2026, 3, 13), Decimal('100.00'), "CANCELAMENTO_COMPRA"),
            ]),
            _cartoes(spark, CARTAO_ATIVO), _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
        )

        fato = construir_fato_transacao(spark, settings)

        assert fato.count() == 1
        linha = fato.collect()[0]
        assert float(linha["valor_bruto"]) == 500.0
        assert float(linha["valor_estornado"]) == 300.0
        assert float(linha["valor_liquido"]) == 200.0
        assert linha["qtd_estornos"] == 2

    def test_transacao_sem_estorno_tem_liquido_igual_ao_bruto(self, spark, settings):
        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
            ]),
            _estornos_vazio(spark),
            _cartoes(spark, CARTAO_ATIVO), _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
        )

        linha = construir_fato_transacao(spark, settings).collect()[0]

        assert float(linha["valor_liquido"]) == 500.0
        assert linha["foi_estornada"] is False
        assert linha["qtd_estornos"] == 0

    def test_estorno_nao_soma_no_valor_liquido_do_mes(self, spark, settings):
        """Cenário obrigatório: transação estornada fora do valor liquido."""
        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
                (2, 10, ts("2026-03-11 10:00:00"), Decimal('300.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 11)),
            ]),
            _estornos(spark, [(900, 1, date(2026, 3, 12), Decimal('500.00'), "FRAUDE")]),
            _cartoes(spark, CARTAO_ATIVO), _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
        )

        fato = construir_fato_transacao(spark, settings)
        mes = construir_cliente_mes(fato, spark, settings).collect()[0]

        assert float(mes["valor_bruto_total"]) == 800.0
        assert float(mes["valor_liquido_total"]) == 300.0
        assert mes["qtd_transacoes_estornadas"] == 1


class TestCartaoCancelado:
    def test_compra_anterior_ao_cancelamento_continua_valendo(self, spark, settings):
        """Cenário obrigatório: cancelado não entra na métrica futura, mas o
        histórico e preservado."""
        cartoes = _cartoes(spark, [
            (10, 100, "CREDITO", Decimal("5000.00"), "ATIVO",
             ts("2025-01-01 00:00:00"), ts("2026-03-15 00:00:00"), False, False),
            (10, 100, "CREDITO", Decimal("5000.00"), "CANCELADO",
             ts("2026-03-15 00:00:00"), None, True, False),
        ])

        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
                (2, 10, ts("2026-03-20 10:00:00"), Decimal('300.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 20)),
            ]),
            _estornos_vazio(spark), cartoes, _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
        )

        fato = {linha["id_transacao"]: linha for linha in construir_fato_transacao(spark, settings).collect()}

        # Antes do cancelamento: cartão ativo na data, entra na métrica.
        assert fato[1]["status_cartao_na_data"] == "ATIVO"
        assert fato[1]["elegivel_metrica"] is True

        # Depois: cartão cancelado na data, sai da métrica mas continua no fato.
        assert fato[2]["status_cartao_na_data"] == "CANCELADO"
        assert fato[2]["elegivel_metrica"] is False

    def test_metrica_mensal_ignora_compra_com_cartao_cancelado(self, spark, settings):
        cartoes = _cartoes(spark, [
            (10, 100, "CREDITO", Decimal("5000.00"), "ATIVO",
             ts("2025-01-01 00:00:00"), ts("2026-03-15 00:00:00"), False, False),
            (10, 100, "CREDITO", Decimal("5000.00"), "CANCELADO",
             ts("2026-03-15 00:00:00"), None, True, False),
        ])

        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
                (2, 10, ts("2026-03-20 10:00:00"), Decimal('300.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 20)),
            ]),
            _estornos_vazio(spark), cartoes, _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
        )

        fato = construir_fato_transacao(spark, settings)
        mes = construir_cliente_mes(fato, spark, settings).collect()[0]

        assert mes["qtd_transacoes"] == 1
        assert float(mes["valor_liquido_total"]) == 500.0

    def test_o_fato_preserva_as_duas_transacoes(self, spark, settings):
        """O cancelamento tira da métrica, não apaga o registro."""
        cartoes = _cartoes(spark, [
            (10, 100, "CREDITO", Decimal("5000.00"), "ATIVO",
             ts("2025-01-01 00:00:00"), ts("2026-03-15 00:00:00"), False, False),
            (10, 100, "CREDITO", Decimal("5000.00"), "CANCELADO",
             ts("2026-03-15 00:00:00"), None, True, False),
        ])

        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
                (2, 10, ts("2026-03-20 10:00:00"), Decimal('300.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 20)),
            ]),
            _estornos_vazio(spark), cartoes, _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
        )

        assert construir_fato_transacao(spark, settings).count() == 2


class TestJoinTemporal:
    def test_transacao_le_o_cadastro_vigente_na_data(self, spark, settings):
        """Cenário obrigatório: o cadastro precisa refletir a versão da epoca.

        Sem isso, uma mudança de segmento em marco reescreveria a apuracao de
        janeiro e o relatório de mês fechado mudaria sozinho.
        """
        clientes = _clientes(spark, [
            (1, "Ana", "Porto Alegre", "RS", "VAREJO", Decimal("5000.00"),
             ts("2025-01-01 00:00:00"), ts("2026-03-15 00:00:00"), False, False),
            (1, "Ana", "Canoas", "RS", "PREMIUM", Decimal("9000.00"),
             ts("2026-03-15 00:00:00"), None, True, False),
        ])

        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
                (2, 10, ts("2026-03-20 10:00:00"), Decimal('300.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 20)),
            ]),
            _estornos_vazio(spark), _cartoes(spark, CARTAO_ATIVO), _contas(spark, CONTA_ATIVA), clientes,
        )

        fato = {linha["id_transacao"]: linha for linha in construir_fato_transacao(spark, settings).collect()}

        assert fato[1]["segmento_na_data"] == "VAREJO"
        assert fato[1]["cidade_na_data"] == "Porto Alegre"
        assert fato[2]["segmento_na_data"] == "PREMIUM"
        assert fato[2]["cidade_na_data"] == "Canoas"

    def test_join_temporal_nao_multiplica_o_fato(self, spark, settings):
        """Três versões do cliente não podem virar três linhas por transação."""
        clientes = _clientes(spark, [
            (1, "Ana", "Porto Alegre", "RS", "VAREJO", Decimal("5000.00"),
             ts("2025-01-01 00:00:00"), ts("2026-02-01 00:00:00"), False, False),
            (1, "Ana", "Canoas", "RS", "PREMIUM", Decimal("7000.00"),
             ts("2026-02-01 00:00:00"), ts("2026-03-15 00:00:00"), False, False),
            (1, "Ana", "Gravatai", "RS", "EMPRESARIAL", Decimal("9000.00"),
             ts("2026-03-15 00:00:00"), None, True, False),
        ])

        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
            ]),
            _estornos_vazio(spark), _cartoes(spark, CARTAO_ATIVO), _contas(spark, CONTA_ATIVA), clientes,
        )

        fato = construir_fato_transacao(spark, settings)

        assert fato.count() == 1
        assert fato.collect()[0]["segmento_na_data"] == "PREMIUM"


class TestIndicadoresRisco:
    def test_calcula_taxa_de_risco_e_de_estorno_por_dia(self, spark, settings):
        eventos = spark.createDataFrame(
            [(500, 1, "FRAUDE_CONFIRMADA", "ALTA", date(2026, 3, 10))],
            ["id_evento", "id_transacao", "tipo_evento", "severidade", "data_evento"],
        )

        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
                (2, 10, ts("2026-03-10 11:00:00"), Decimal('300.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
                (3, 10, ts("2026-03-10 12:00:00"), Decimal('200.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
                (4, 10, ts("2026-03-10 13:00:00"), Decimal('100.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
            ]),
            _estornos(spark, [(900, 2, date(2026, 3, 11), Decimal('300.00'), "FRAUDE")]),
            _cartoes(spark, CARTAO_ATIVO), _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
            eventos=eventos,
        )

        fato = construir_fato_transacao(spark, settings)
        risco = construir_indicadores_risco(fato, spark, settings).collect()[0]

        assert risco["qtd_transacoes"] == 4
        assert risco["qtd_fraude_confirmada"] == 1
        assert risco["qtd_com_evento_risco"] == 1
        assert risco["qtd_estornadas"] == 1
        assert risco["taxa_risco_pct"] == 25.0
        assert risco["taxa_estorno_pct"] == 25.0

    def test_fraude_em_cartao_cancelado_continua_no_indicador(self, spark, settings):
        """Risco olha a transação inteira, inclusive a não elegível para métrica."""
        cartoes = _cartoes(spark, [
            (10, 100, "CREDITO", Decimal("5000.00"), "CANCELADO", ts("2025-01-01 00:00:00"), None, True, False),
        ])
        eventos = spark.createDataFrame(
            [(500, 1, "FRAUDE_CONFIRMADA", "ALTA", date(2026, 3, 10))],
            ["id_evento", "id_transacao", "tipo_evento", "severidade", "data_evento"],
        )

        _montar_prata(
            spark, settings,
            _transacoes(spark, [
                (1, 10, ts("2026-03-10 10:00:00"), Decimal('500.00'), 5411, "Mercado", "POS", "BR", "BRL", "CHIP", date(2026, 3, 10)),
            ]),
            _estornos_vazio(spark), cartoes, _contas(spark, CONTA_ATIVA), _clientes(spark, CLIENTE_BASE),
            eventos=eventos,
        )

        fato = construir_fato_transacao(spark, settings)
        risco = construir_indicadores_risco(fato, spark, settings).collect()[0]

        assert risco["qtd_fraude_confirmada"] == 1
        assert float(risco["valor_em_fraude"]) == 500.0
