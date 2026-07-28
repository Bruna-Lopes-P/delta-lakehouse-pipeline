"""Testes de tipagem e deduplicação da Prata."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import functions as F

from src.silver.dimensions import tipar_cartao, tipar_cliente, tipar_conta
from src.silver.facts import deduplicar_por_chave, tipar_transacao


def ts(texto: str) -> datetime:
    return datetime.fromisoformat(texto)


def _bronze_cliente(spark, linhas):
    colunas = [
        "id_cliente", "cpf", "nome", "cidade", "estado", "renda", "segmento",
        "data_atualizacao", "operacao", "arquivo_origem", "batch_id",
    ]
    return spark.createDataFrame(linhas, colunas)


class TestTipagemCliente:
    def test_cpf_permanece_string(self, spark):
        """Convertido para número, o CPF perderia o zero a esquerda."""
        df = _bronze_cliente(spark, [
            ("1", "00012345678", "Ana", "Porto Alegre", "RS", "5000.50",
             "VAREJO", "2026-01-10 08:00:00", "INSERT", "arq.json", "b1")
        ])

        linha = tipar_cliente(df).collect()[0]

        assert linha["cpf"] == "00012345678"
        assert isinstance(linha["cpf"], str)

    def test_converte_id_renda_e_data(self, spark):
        df = _bronze_cliente(spark, [
            ("42", "00012345678", "Ana", "Porto Alegre", "RS", "5000.50",
             "VAREJO", "2026-01-10 08:00:00", "INSERT", "arq.json", "b1")
        ])

        linha = tipar_cliente(df).collect()[0]

        assert linha["id_cliente"] == 42
        assert float(linha["renda"]) == 5000.50
        assert linha["data_atualizacao"] == ts("2026-01-10 08:00:00")

    def test_normaliza_texto(self, spark):
        """Espaco e caixa vindos da origem não podem virar categoria diferente."""
        df = _bronze_cliente(spark, [
            ("1", " 00012345678 ", "  Ana  ", " Porto Alegre ", " rs ", "5000",
             " varejo ", "2026-01-10 08:00:00", " insert ", "arq.json", "b1")
        ])

        linha = tipar_cliente(df).collect()[0]

        assert linha["cpf"] == "00012345678"
        assert linha["nome"] == "Ana"
        assert linha["cidade"] == "Porto Alegre"
        assert linha["estado"] == "RS"
        assert linha["segmento"] == "VAREJO"
        assert linha["operacao"] == "INSERT"

    def test_data_invalida_vira_nulo_e_nao_quebra(self, spark):
        """A regra de qualidade rejeita depois; a tipagem não pode derrubar a carga."""
        df = _bronze_cliente(spark, [
            ("1", "00012345678", "Ana", "Porto Alegre", "RS", "5000",
             "VAREJO", "data-invalida", "INSERT", "arq.json", "b1")
        ])

        assert tipar_cliente(df).collect()[0]["data_atualizacao"] is None


class TestTipagemConta:
    def test_converte_data_de_abertura_para_date(self, spark):
        df = spark.createDataFrame(
            [("1", "10", "corrente", "ativa", "2024-05-20", "2026-01-10 08:00:00",
              "INSERT", "arq.csv", "b1")],
            ["id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura",
             "data_atualizacao", "operacao", "arquivo_origem", "batch_id"],
        )

        linha = tipar_conta(df).collect()[0]

        assert linha["data_abertura"].isoformat() == "2024-05-20"
        assert linha["tipo_conta"] == "CORRENTE"
        assert linha["status_conta"] == "ATIVA"


class TestTipagemCartao:
    def test_converte_limite_para_decimal(self, spark):
        df = spark.createDataFrame(
            [("1", "10", "credito", "1500.75", "ativo", "2026-01-10 08:00:00",
              "INSERT", "arq.csv", "b1")],
            ["id_cartao", "id_conta", "tipo_cartao", "limite", "status_cartao",
             "data_atualizacao", "operacao", "arquivo_origem", "batch_id"],
        )

        linha = tipar_cartao(df).collect()[0]

        assert float(linha["limite"]) == 1500.75
        assert linha["status_cartao"] == "ATIVO"


class TestTipagemTransacao:
    def _bronze(self, spark, com_dispositivo: bool):
        colunas = ["id_transacao", "id_cartao", "data_transacao", "valor", "mcc",
                   "estabelecimento", "canal", "pais", "moeda",
                   "arquivo_origem", "batch_id", "timestamp_ingestao"]
        valores = ["1", "10", "2026-03-15 14:30:00", "250.90", "5411",
                   " Mercado Central ", "pos", "br", "brl",
                   "arq.csv", "b1", ts("2026-03-16 02:00:00")]

        if com_dispositivo:
            colunas.insert(9, "dispositivo")
            valores.insert(9, "chip")

        return spark.createDataFrame([tuple(valores)], colunas)

    def test_deriva_a_data_de_particao_da_transacao(self, spark):
        """A partição segue a data do fato, não a da carga."""
        linha = tipar_transacao(self._bronze(spark, False)).collect()[0]

        assert linha["data_particao"].isoformat() == "2026-03-15"
        assert linha["data_transacao"] == ts("2026-03-15 14:30:00")

    def test_funciona_sem_a_coluna_dispositivo(self, spark):
        """Arquivos antigos não tem a coluna e não podem quebrar a carga."""
        linha = tipar_transacao(self._bronze(spark, False)).collect()[0]

        assert linha["dispositivo"] is None
        assert float(linha["valor"]) == 250.90

    def test_usa_a_coluna_dispositivo_quando_ela_existe(self, spark):
        linha = tipar_transacao(self._bronze(spark, True)).collect()[0]

        assert linha["dispositivo"] == "CHIP"


class TestDeduplicacao:
    def test_mantem_a_ingestao_mais_recente(self, spark):
        """Reenvio da origem: vence a última versão recebida."""
        df = spark.createDataFrame(
            [
                (1, 100.0, ts("2026-03-16 02:00:00")),
                (1, 150.0, ts("2026-03-17 02:00:00")),
                (2, 200.0, ts("2026-03-16 02:00:00")),
            ],
            ["id_transacao", "valor", "timestamp_ingestao"],
        )

        resultado = deduplicar_por_chave(df, ["id_transacao"])

        assert resultado.count() == 2
        vencedora = resultado.filter(F.col("id_transacao") == 1).collect()[0]
        assert vencedora["valor"] == 150.0

    def test_mesma_transacao_em_dois_arquivos_vira_uma_linha(self, spark):
        """Cenário obrigatório: id_transacao repetido entre cargas diferentes."""
        df = spark.createDataFrame(
            [
                (1, "arq_dia1.csv", ts("2026-03-16 02:00:00")),
                (1, "arq_dia2.csv", ts("2026-03-17 02:00:00")),
            ],
            ["id_transacao", "arquivo_origem", "timestamp_ingestao"],
        )

        resultado = deduplicar_por_chave(df, ["id_transacao"])

        assert resultado.count() == 1
        assert resultado.collect()[0]["arquivo_origem"] == "arq_dia2.csv"

    def test_nao_remove_chaves_distintas(self, spark):
        df = spark.createDataFrame(
            [(1, ts("2026-03-16 02:00:00")), (2, ts("2026-03-16 02:00:00")),
             (3, ts("2026-03-16 02:00:00"))],
            ["id_transacao", "timestamp_ingestao"],
        )

        assert deduplicar_por_chave(df, ["id_transacao"]).count() == 3
