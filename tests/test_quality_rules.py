"""Testes das regras de qualidade e da quarentena."""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F

from src.quality.quarantine import gravar_quarentena, separar_orfaos
from src.quality.rules import (
    Regra,
    aplicar_regras,
    regras_cartao,
    regras_cliente,
    regras_transacao,
)
from src.utils.spark import ler_delta

ESQUEMA_CLIENTE = (
    "id_cliente long, cpf string, nome string, cidade string, estado string, "
    "renda double, segmento string, data_atualizacao date"
)


def _clientes(spark):
    return spark.createDataFrame(
        [
            (1, "00000000001", "Ana", "Porto Alegre", "RS", 5000.0, "VAREJO", date(2026, 1, 10)),
            (2, None, "Bruno", "Curitiba", "PR", 4000.0, "VAREJO", date(2026, 1, 10)),
            (3, "123", "Carla", "Sao Paulo", "SP", 6000.0, "PREMIUM", date(2026, 1, 10)),
            (4, "00000000004", "Davi", "Recife", "ZZ", 3000.0, "VAREJO", date(2026, 1, 10)),
            (5, "00000000005", "Eva", "Curitiba", "PR", -100.0, "VAREJO", date(2026, 1, 10)),
        ],
        ["id_cliente", "cpf", "nome", "cidade", "estado", "renda", "segmento", "data_atualizacao"],
    )


class TestAplicarRegras:
    def test_separa_aprovados_de_rejeitados(self, spark):
        aprovados, rejeitados = aplicar_regras(_clientes(spark), regras_cliente())

        assert aprovados.count() == 1
        assert rejeitados.count() == 4
        assert aprovados.collect()[0]["id_cliente"] == 1

    def test_registra_o_motivo_de_cada_rejeicao(self, spark):
        _, rejeitados = aplicar_regras(_clientes(spark), regras_cliente())

        motivos = {
            linha["id_cliente"]: set(linha["motivos_rejeicao"])
            for linha in rejeitados.collect()
        }

        # CPF nulo viola obrigatoriedade e formato ao mesmo tempo.
        assert motivos[2] == {"cliente_cpf_obrigatorio", "cliente_cpf_11_digitos"}
        assert motivos[3] == {"cliente_cpf_11_digitos"}
        assert motivos[4] == {"cliente_uf_valida"}
        assert motivos[5] == {"cliente_renda_nao_negativa"}

    def test_linha_pode_violar_mais_de_uma_regra(self, spark):
        _, rejeitados = aplicar_regras(_clientes(spark), regras_cliente())
        linha = rejeitados.filter(F.col("id_cliente") == 2).collect()[0]

        assert len(linha["motivos_rejeicao"]) == 2

    def test_nulo_na_condicao_conta_como_violacao(self, spark):
        """Comparação com nulo devolve NULL, e NULL não e True.

        Sem o coalesce em aplicar_regras a linha escaparia da regra em vez de
        ser rejeitada, que é o comportamento silencioso que se quer evitar.
        """
        df = spark.createDataFrame([(1, None), (2, 50.0)], ["id", "valor"])
        regra = Regra("valor_maior_que_10", "valor precisa passar de 10", F.col("valor") > 10)

        aprovados, rejeitados = aplicar_regras(df, [regra])

        assert aprovados.count() == 1
        assert rejeitados.count() == 1
        assert rejeitados.collect()[0]["id"] == 1

    def test_sem_regras_aprova_tudo(self, spark):
        df = _clientes(spark)
        aprovados, rejeitados = aplicar_regras(df, [])

        assert aprovados.count() == df.count()
        assert rejeitados.count() == 0


class TestRegrasDeTransacao:
    def test_rejeita_valor_zero_e_negativo(self, spark):
        df = spark.createDataFrame(
            [
                (1, 10, "2026-03-01 10:00:00", 100.0, "BRL"),
                (2, 10, "2026-03-01 11:00:00", 0.0, "BRL"),
                (3, 10, "2026-03-01 12:00:00", -50.0, "BRL"),
            ],
            ["id_transacao", "id_cartao", "data_transacao", "valor", "moeda"],
        ).withColumn("data_transacao", F.to_timestamp("data_transacao"))

        aprovados, rejeitados = aplicar_regras(df, regras_transacao())

        assert aprovados.count() == 1
        assert {linha["id_transacao"] for linha in rejeitados.collect()} == {2, 3}

    def test_rejeita_moeda_desconhecida(self, spark):
        df = spark.createDataFrame(
            [(1, 10, "2026-03-01 10:00:00", 100.0, "XPT")],
            ["id_transacao", "id_cartao", "data_transacao", "valor", "moeda"],
        ).withColumn("data_transacao", F.to_timestamp("data_transacao"))

        aprovados, rejeitados = aplicar_regras(df, regras_transacao())

        assert aprovados.count() == 0
        assert "transacao_moeda_conhecida" in rejeitados.collect()[0]["motivos_rejeicao"]


class TestRegrasDeCartao:
    def test_rejeita_limite_negativo_e_aceita_nulo(self, spark):
        df = spark.createDataFrame(
            [
                (1, 100, "CREDITO", 5000.0, "ATIVO"),
                (2, 100, "CREDITO", -1.0, "ATIVO"),
                (3, 100, "DEBITO", None, "ATIVO"),
            ],
            ["id_cartao", "id_conta", "tipo_cartao", "limite", "status_cartao"],
        )

        aprovados, rejeitados = aplicar_regras(df, regras_cartao())

        assert {linha["id_cartao"] for linha in aprovados.collect()} == {1, 3}
        assert rejeitados.collect()[0]["id_cartao"] == 2


class TestIntegridadeReferencial:
    def test_separa_orfaos_de_integros(self, spark):
        cartoes = spark.createDataFrame(
            [(1, 100), (2, 200), (3, 999)], ["id_cartao", "id_conta"]
        )
        contas = spark.createDataFrame([(100,), (200,)], ["id_conta"])

        integros, orfaos = separar_orfaos(
            cartoes, contas, "id_conta", "id_conta", "cartao_sem_conta_valida"
        )

        assert {linha["id_cartao"] for linha in integros.collect()} == {1, 2}
        assert orfaos.count() == 1
        assert orfaos.collect()[0]["id_cartao"] == 3

    def test_orfao_recebe_o_motivo(self, spark):
        cartoes = spark.createDataFrame([(3, 999)], ["id_cartao", "id_conta"])
        contas = spark.createDataFrame([(100,)], ["id_conta"])

        _, orfaos = separar_orfaos(
            cartoes, contas, "id_conta", "id_conta", "cartao_sem_conta_valida"
        )

        assert orfaos.collect()[0]["motivos_rejeicao"] == ["cartao_sem_conta_valida"]

    def test_nao_multiplica_linhas_quando_o_pai_tem_duplicata(self, spark):
        """Left semi não duplica o filho mesmo com chave repetida no pai."""
        cartoes = spark.createDataFrame([(1, 100)], ["id_cartao", "id_conta"])
        contas = spark.createDataFrame([(100,), (100,), (100,)], ["id_conta"])

        integros, _ = separar_orfaos(
            cartoes, contas, "id_conta", "id_conta", "cartao_sem_conta_valida"
        )

        assert integros.count() == 1


class TestQuarentena:
    def test_grava_rejeitados_com_motivo_e_registro_original(self, spark, settings):
        _, rejeitados = aplicar_regras(_clientes(spark), regras_cliente())

        quantidade = gravar_quarentena(rejeitados, "cliente", settings)

        assert quantidade == 4

        gravado = ler_delta(
            spark, settings.tabela("quarentena", "registros_rejeitados"), settings
        )
        assert gravado.count() == 4

        linha = gravado.collect()[0]
        assert linha["entidade"] == "cliente"
        assert linha["batch_id"] == settings.batch_id
        # O registro original fica recuperável para reinjeção depois da correção.
        assert '"id_cliente"' in linha["registro_original"]

    def test_gravar_duas_vezes_nao_duplica(self, spark, settings):
        """A Prata reprocessa a Bronze inteira a cada execução, então os mesmos
        registros inválidos são reprovados de novo. Com append, a quarentena
        dobraria a cada reprocessamento."""
        _, rejeitados = aplicar_regras(_clientes(spark), regras_cliente())

        gravar_quarentena(rejeitados, "cliente", settings)
        gravar_quarentena(rejeitados, "cliente", settings)

        gravado = ler_delta(
            spark, settings.tabela("quarentena", "registros_rejeitados"), settings
        )
        assert gravado.count() == 4

    def test_rejeicao_por_outro_motivo_entra_como_nova(self, spark, settings):
        """O hash cobre os motivos: se o registro passa a violar outra regra, e
        outra rejeição, e não a mesma."""
        df = spark.createDataFrame(
            [(1, None, "Ana", "Porto Alegre", "RS", 5000.0, "VAREJO", date(2026, 1, 10))],
            ESQUEMA_CLIENTE,
        )
        _, rejeitados = aplicar_regras(df, regras_cliente())
        gravar_quarentena(rejeitados, "cliente", settings)

        # Mesmo id, agora com CPF válido mas UF inválida.
        outro = spark.createDataFrame(
            [(1, "00000000001", "Ana", "Porto Alegre", "ZZ", 5000.0, "VAREJO", date(2026, 1, 10))],
            ESQUEMA_CLIENTE,
        )
        _, rejeitados_outro = aplicar_regras(outro, regras_cliente())
        gravar_quarentena(rejeitados_outro, "cliente", settings)

        gravado = ler_delta(
            spark, settings.tabela("quarentena", "registros_rejeitados"), settings
        )
        assert gravado.count() == 2

    def test_nao_grava_nada_quando_nao_ha_rejeitado(self, spark, settings):
        df = spark.createDataFrame(
            [(1, "00000000001", "Ana", "Porto Alegre", "RS", 5000.0, "VAREJO", date(2026, 1, 10))],
            ["id_cliente", "cpf", "nome", "cidade", "estado", "renda", "segmento", "data_atualizacao"],
        )
        _, rejeitados = aplicar_regras(df, regras_cliente())

        assert gravar_quarentena(rejeitados, "cliente", settings) == 0

    def test_exige_a_coluna_de_motivos(self, spark, settings):
        """Gravar sem motivo produziria quarentena inutil, então falha cedo."""
        df = spark.createDataFrame([(1,)], ["id_cliente"])

        with pytest.raises(ValueError, match="motivos_rejeicao"):
            gravar_quarentena(df, "cliente", settings)
