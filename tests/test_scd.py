"""Testes do SCD Tipo 2.

Cobrem o comportamento essencial: controle de vigência, registro da
versão corrente, preservacao das versões antigas, consulta pontual por data e
idempotência.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import functions as F

from src.silver.scd import (
    aplicar_scd2,
    preparar_versoes,
    versao_corrente,
    versao_vigente_em,
)
from src.utils.spark import ler_delta

CHAVE = ["id_cliente"]
ATRIBUTOS = ["nome", "cidade", "segmento"]


def ts(texto: str) -> datetime:
    return datetime.fromisoformat(texto)


def _clientes(spark, linhas):
    return spark.createDataFrame(
        linhas,
        ["id_cliente", "nome", "cidade", "segmento", "data_atualizacao", "operacao"],
    )


def _carga_inicial(spark):
    return _clientes(
        spark,
        [
            (1, "Ana", "Porto Alegre", "VAREJO", ts("2026-01-10 08:00:00"), "INSERT"),
            (2, "Bruno", "Curitiba", "VAREJO", ts("2026-01-10 08:00:00"), "INSERT"),
        ],
    )


def _aplicar(spark, settings, df):
    return aplicar_scd2(
        spark, df, "dim_cliente", settings,
        chave_negocio=CHAVE,
        coluna_versao="data_atualizacao",
        colunas_atributos=ATRIBUTOS,
        coluna_operacao="operacao",
    )


def _ler(spark, settings):
    return ler_delta(spark, settings.tabela("silver", "dim_cliente"), settings)


class TestPrimeiraCarga:
    def test_cria_a_dimensao_com_versao_ativa(self, spark, settings):
        resultado = _aplicar(spark, settings, _carga_inicial(spark))

        assert resultado["versoes_novas"] == 2

        dim = _ler(spark, settings)
        assert dim.count() == 2
        assert dim.filter(F.col("dw_versao_ativa")).count() == 2
        assert dim.filter(F.col("dw_fim_vigencia").isNotNull()).count() == 0

    def test_vigencia_inicial_vem_da_origem_e_nao_da_data_da_carga(self, spark, settings):
        """A data do fato manda, não a data em que o arquivo chegou.

        Sem isso, uma alteração ocorrida há três dias e carregada hoje ficaria
        registrada como se tivesse acontecido hoje, e a consulta pontual daria
        a resposta errada para os dias intermediarios.
        """
        _aplicar(spark, settings, _carga_inicial(spark))

        linha = _ler(spark, settings).filter(F.col("id_cliente") == 1).collect()[0]
        assert linha["dw_inicio_vigencia"] == ts("2026-01-10 08:00:00")


class TestNovaVersao:
    def test_mudanca_de_atributo_abre_versao_e_fecha_a_anterior(self, spark, settings):
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Porto Alegre", "PREMIUM", ts("2026-02-15 09:00:00"), "UPDATE")
            ]),
        )

        versoes = _ler(spark, settings).filter(F.col("id_cliente") == 1).orderBy("dw_inicio_vigencia").collect()

        assert len(versoes) == 2

        antiga, nova = versoes
        assert antiga["segmento"] == "VAREJO"
        assert antiga["dw_versao_ativa"] is False
        assert antiga["dw_fim_vigencia"] == ts("2026-02-15 09:00:00")

        assert nova["segmento"] == "PREMIUM"
        assert nova["dw_versao_ativa"] is True
        assert nova["dw_fim_vigencia"] is None

    def test_intervalos_nao_se_sobrepoem(self, spark, settings):
        """O fim de uma versão é exatamente o início da seguinte.

        Com intervalo fechado no início e aberto no fim, isso não gera
        ambiguidade: no instante do corte vale a versão nova.
        """
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Canoas", "PREMIUM", ts("2026-02-15 09:00:00"), "UPDATE")
            ]),
        )

        versoes = _ler(spark, settings).filter(F.col("id_cliente") == 1).orderBy("dw_inicio_vigencia").collect()
        assert versoes[0]["dw_fim_vigencia"] == versoes[1]["dw_inicio_vigencia"]

    def test_versao_de_outra_chave_nao_e_afetada(self, spark, settings):
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Porto Alegre", "PREMIUM", ts("2026-02-15 09:00:00"), "UPDATE")
            ]),
        )

        bruno = _ler(spark, settings).filter(F.col("id_cliente") == 2).collect()
        assert len(bruno) == 1
        assert bruno[0]["dw_versao_ativa"] is True


class TestIdempotencia:
    def test_reprocessar_a_mesma_carga_nao_cria_versao(self, spark, settings):
        """Rodar três vezes o mesmo arquivo produz a mesma tabela."""
        _aplicar(spark, settings, _carga_inicial(spark))
        antes = _ler(spark, settings).count()

        segunda = _aplicar(spark, settings, _carga_inicial(spark))
        terceira = _aplicar(spark, settings, _carga_inicial(spark))

        assert segunda["versoes_novas"] == 0
        assert segunda["ignorados_sem_mudanca"] == 2
        assert terceira["versoes_novas"] == 0
        assert _ler(spark, settings).count() == antes

    def test_duplicata_exata_na_mesma_carga_vira_uma_versao(self, spark, settings):
        duplicada = _clientes(spark, [
            (1, "Ana", "Porto Alegre", "VAREJO", ts("2026-01-10 08:00:00"), "INSERT"),
            (1, "Ana", "Porto Alegre", "VAREJO", ts("2026-01-10 08:00:00"), "INSERT"),
        ])

        _aplicar(spark, settings, duplicada)

        assert _ler(spark, settings).filter(F.col("id_cliente") == 1).count() == 1

    def test_carga_sem_mudanca_preserva_a_vigencia_original(self, spark, settings):
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(spark, settings, _carga_inicial(spark))

        linha = _ler(spark, settings).filter(F.col("id_cliente") == 1).collect()[0]
        assert linha["dw_inicio_vigencia"] == ts("2026-01-10 08:00:00")
        assert linha["dw_fim_vigencia"] is None


class TestDadoAtrasado:
    def test_registro_anterior_ao_vigente_e_descartado(self, spark, settings):
        """Aplicar uma alteração já superada abriria versão no passado.

        A linha do tempo passaria a ter a versão antiga como corrente, o que
        contradiz o dado que já esta na dimensão.
        """
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Porto Alegre", "PREMIUM", ts("2026-02-15 09:00:00"), "UPDATE")
            ]),
        )

        atrasado = _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Porto Alegre", "EMPRESARIAL", ts("2026-01-20 07:00:00"), "UPDATE")
            ]),
        )

        assert atrasado["ignorados_atrasados"] == 1
        assert atrasado["versoes_novas"] == 0

        corrente = versao_corrente(_ler(spark, settings)).filter(F.col("id_cliente") == 1).collect()[0]
        assert corrente["segmento"] == "PREMIUM"

    def test_conteudo_diferente_no_mesmo_instante_da_vigente_e_ignorado(self, spark, settings):
        """Aplicar produziria um intervalo de vigência vazio.

        A versão vigente seria fechada com dw_fim_vigencia igual ao próprio
        dw_inicio_vigencia, um intervalo [X, X) que não responde por instante
        nenhum. Corrigir um evento já gravado exige reprocessar a entidade a
        partir da Bronze.
        """
        _aplicar(spark, settings, _carga_inicial(spark))

        conflito = _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Gravatai", "EMPRESARIAL", ts("2026-01-10 08:00:00"), "UPDATE")
            ]),
        )

        assert conflito["versoes_novas"] == 0
        assert conflito["ignorados_atrasados"] == 1

        versoes = _ler(spark, settings).filter(F.col("id_cliente") == 1).collect()
        assert len(versoes) == 1
        assert versoes[0]["cidade"] == "Porto Alegre"

    def test_nenhum_intervalo_de_vigencia_fica_vazio(self, spark, settings):
        """Invariante: dw_fim_vigencia sempre maior que dw_inicio_vigencia."""
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Canoas", "PREMIUM", ts("2026-02-15 09:00:00"), "UPDATE")
            ]),
        )
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Canoas", "AGRO", ts("2026-01-10 08:00:00"), "UPDATE")
            ]),
        )

        degenerados = _ler(spark, settings).filter(
            F.col("dw_fim_vigencia").isNotNull()
            & (F.col("dw_fim_vigencia") <= F.col("dw_inicio_vigencia"))
        )
        assert degenerados.count() == 0


class TestVariasVersoesNaMesmaCarga:
    def test_carga_com_tres_versoes_encadeia_todas(self, spark, settings):
        """O CDC pode trazer INSERT e dois UPDATE do mesmo cliente de uma vez."""
        carga = _clientes(spark, [
            (1, "Ana", "Porto Alegre", "VAREJO", ts("2026-01-10 08:00:00"), "INSERT"),
            (1, "Ana", "Canoas", "VAREJO", ts("2026-02-01 08:00:00"), "UPDATE"),
            (1, "Ana", "Canoas", "PREMIUM", ts("2026-03-01 08:00:00"), "UPDATE"),
        ])

        _aplicar(spark, settings, carga)

        versoes = _ler(spark, settings).orderBy("dw_inicio_vigencia").collect()

        assert len(versoes) == 3
        assert [v["dw_versao_ativa"] for v in versoes] == [False, False, True]
        assert versoes[0]["dw_fim_vigencia"] == versoes[1]["dw_inicio_vigencia"]
        assert versoes[1]["dw_fim_vigencia"] == versoes[2]["dw_inicio_vigencia"]
        assert versoes[2]["dw_fim_vigencia"] is None

    def test_duas_versoes_novas_sobre_dimensao_existente(self, spark, settings):
        """Caso que quebra um MERGE ingenuo.

        Com a dimensão já materializada, duas versões novas da mesma chave
        gerariam duas linhas de fechamento apontando para a mesma versão vigente,
        e o Delta aborta com erro de múltiplas linhas atingindo o mesmo alvo.
        Só a primeira das novas pode fechar a vigente; as demais se encadeiam
        entre si.
        """
        _aplicar(spark, settings, _carga_inicial(spark))

        resultado = _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Canoas", "VAREJO", ts("2026-02-01 08:00:00"), "UPDATE"),
                (1, "Ana", "Canoas", "PREMIUM", ts("2026-03-01 08:00:00"), "UPDATE"),
            ]),
        )

        assert resultado["versoes_novas"] == 2

        versoes = (
            _ler(spark, settings)
            .filter(F.col("id_cliente") == 1)
            .orderBy("dw_inicio_vigencia")
            .collect()
        )

        assert len(versoes) == 3
        assert [v["dw_versao_ativa"] for v in versoes] == [False, False, True]

        # A linha do tempo permanece contínua atravessando a fronteira entre a
        # versão que já estava na tabela e as que chegaram agora.
        assert versoes[0]["dw_fim_vigencia"] == versoes[1]["dw_inicio_vigencia"]
        assert versoes[1]["dw_fim_vigencia"] == versoes[2]["dw_inicio_vigencia"]
        assert versoes[2]["segmento"] == "PREMIUM"

    def test_duas_chaves_com_duas_versoes_cada(self, spark, settings):
        """O mesmo caso acima, com mais de uma entidade na carga."""
        _aplicar(spark, settings, _carga_inicial(spark))

        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Canoas", "VAREJO", ts("2026-02-01 08:00:00"), "UPDATE"),
                (1, "Ana", "Canoas", "PREMIUM", ts("2026-03-01 08:00:00"), "UPDATE"),
                (2, "Bruno", "Pelotas", "VAREJO", ts("2026-02-05 08:00:00"), "UPDATE"),
                (2, "Bruno", "Pelotas", "AGRO", ts("2026-03-05 08:00:00"), "UPDATE"),
            ]),
        )

        dim = _ler(spark, settings)

        for id_cliente in (1, 2):
            versoes = dim.filter(F.col("id_cliente") == id_cliente).orderBy("dw_inicio_vigencia").collect()
            assert len(versoes) == 3, f"cliente {id_cliente}"
            assert sum(1 for v in versoes if v["dw_versao_ativa"]) == 1


class TestExclusao:
    def test_delete_encerra_a_vigencia_sem_deixar_versao_ativa(self, spark, settings):
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (2, "Bruno", "Curitiba", "VAREJO", ts("2026-02-20 10:00:00"), "DELETE")
            ]),
        )

        bruno = _ler(spark, settings).filter(F.col("id_cliente") == 2).collect()

        assert len(bruno) == 2
        assert versao_corrente(_ler(spark, settings)).filter(F.col("id_cliente") == 2).count() == 0
        assert any(linha["dw_excluido"] for linha in bruno)

    def test_delete_preserva_o_historico(self, spark, settings):
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (2, "Bruno", "Curitiba", "VAREJO", ts("2026-02-20 10:00:00"), "DELETE")
            ]),
        )

        # O cadastro de janeiro continua consultavel depois da exclusão.
        em_janeiro = versao_vigente_em(_ler(spark, settings), F.lit(ts("2026-01-15 00:00:00")))
        assert em_janeiro.filter(F.col("id_cliente") == 2).count() == 1


class TestConsultaPontual:
    def test_recupera_a_versao_valida_em_cada_data(self, spark, settings):
        """E o requisito central do SCD Tipo 2: qual era o cadastro naquele dia."""
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Porto Alegre", "PREMIUM", ts("2026-02-15 09:00:00"), "UPDATE")
            ]),
        )
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Porto Alegre", "EMPRESARIAL", ts("2026-03-20 09:00:00"), "UPDATE")
            ]),
        )

        dim = _ler(spark, settings)

        def segmento_em(momento: str) -> str:
            return (
                versao_vigente_em(dim, F.lit(ts(momento)))
                .filter(F.col("id_cliente") == 1)
                .collect()[0]["segmento"]
            )

        assert segmento_em("2026-01-20 00:00:00") == "VAREJO"
        assert segmento_em("2026-02-20 00:00:00") == "PREMIUM"
        assert segmento_em("2026-03-25 00:00:00") == "EMPRESARIAL"

    def test_no_instante_do_corte_vale_a_versao_nova(self, spark, settings):
        _aplicar(spark, settings, _carga_inicial(spark))
        _aplicar(
            spark, settings,
            _clientes(spark, [
                (1, "Ana", "Porto Alegre", "PREMIUM", ts("2026-02-15 09:00:00"), "UPDATE")
            ]),
        )

        no_corte = versao_vigente_em(_ler(spark, settings), F.lit(ts("2026-02-15 09:00:00")))
        linhas = no_corte.filter(F.col("id_cliente") == 1).collect()

        # Exatamente uma versão responde por qualquer instante: sem buraco e sem
        # sobreposição.
        assert len(linhas) == 1
        assert linhas[0]["segmento"] == "PREMIUM"

    def test_antes_da_primeira_versao_nao_retorna_nada(self, spark, settings):
        _aplicar(spark, settings, _carga_inicial(spark))

        antes = versao_vigente_em(_ler(spark, settings), F.lit(ts("2025-12-01 00:00:00")))
        assert antes.count() == 0


class TestPrepararVersoes:
    def test_hash_distingue_nulo_em_posicoes_diferentes(self, spark):
        """(A, nulo) e (nulo, A) precisam ter hashes diferentes.

        Com concatenação simples os dois virariam a mesma string e uma mudança
        real passaria despercebida.
        """
        df = spark.createDataFrame(
            [
                (1, "A", None, ts("2026-01-01 00:00:00"), "INSERT"),
                (2, None, "A", ts("2026-01-01 00:00:00"), "INSERT"),
            ],
            ["id_cliente", "nome", "cidade", "data_atualizacao", "operacao"],
        )

        preparado = preparar_versoes(
            df, ["id_cliente"], "data_atualizacao", ["nome", "cidade"], "operacao"
        ).collect()

        assert preparado[0]["dw_hash_atributos"] != preparado[1]["dw_hash_atributos"]

    def test_conflito_no_mesmo_instante_resolve_de_forma_deterministica(self, spark):
        """Duas versões diferentes no mesmo instante não podem virar duas linhas."""
        df = spark.createDataFrame(
            [
                (1, "Ana", "Porto Alegre", ts("2026-01-10 08:00:00"), "INSERT"),
                (1, "Ana", "Curitiba", ts("2026-01-10 08:00:00"), "INSERT"),
            ],
            ["id_cliente", "nome", "cidade", "data_atualizacao", "operacao"],
        )

        primeira = preparar_versoes(df, ["id_cliente"], "data_atualizacao", ["nome", "cidade"], "operacao")
        segunda = preparar_versoes(df, ["id_cliente"], "data_atualizacao", ["nome", "cidade"], "operacao")

        assert primeira.count() == 1
        # Duas execuções escolhem a mesma linha, o que preserva a idempotência.
        assert primeira.collect()[0]["cidade"] == segunda.collect()[0]["cidade"]
