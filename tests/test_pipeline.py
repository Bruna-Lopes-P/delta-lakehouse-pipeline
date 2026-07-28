"""Testes de ponta a ponta.

Rodam o pipeline inteiro sobre a massa sintética, o que exercita a integração
entre as camadas. São mais lentos que os testes de unidade e pegam justamente o
que eles não pegam: contrato entre módulos e idempotência real do conjunto.

A maioria dos casos só faz asserção de leitura sobre o resultado, então uma
única execução do pipeline e compartilhada por eles (fixture de escopo de
módulo). Os casos que precisam controlar quantas vezes o pipeline rodou usam
ambiente próprio, isolado.
"""

from __future__ import annotations

import shutil
import tempfile

import pytest
from pyspark.sql import functions as F

from src.config.settings import settings_de_teste
from src.datagen.generator import gerar_massa
from src.ingestion.bronze import arquivos_ja_ingeridos, ingerir
from src.pipeline.run import executar
from src.utils.spark import ler_delta
from tests.conftest import DATA_REFERENCIA

QTD_CLIENTES = 40
DIAS = 3


def _tabela(spark, settings, camada, nome):
    return ler_delta(spark, settings.tabela(camada, nome), settings)


@pytest.fixture(scope="module")
def ambiente_executado(spark):
    """Gera a massa e roda o pipeline uma vez, para os casos de leitura.

    Escopo de módulo porque subir o pipeline inteiro custa minutos e os casos
    que consomem esta fixture apenas leem o resultado, sem alterar nada.
    """
    raiz = tempfile.mkdtemp(prefix="novarota-e2e-").replace("\\", "/")
    settings = settings_de_teste(raiz, DATA_REFERENCIA)

    gerar_massa(settings, qtd_clientes=QTD_CLIENTES, dias=DIAS)
    resumo = executar(settings, spark)

    yield settings, resumo

    shutil.rmtree(raiz, ignore_errors=True)


class TestGeracaoDeMassa:
    def test_gera_todas_as_origens(self, settings):
        resumo = gerar_massa(settings, qtd_clientes=30, dias=3)

        assert resumo["clientes"] > 30  # base mais as versoes e os invalidos
        assert resumo["contas"] > 0
        assert resumo["cartoes"] > 0
        assert resumo["transacoes"] > 0
        assert resumo["estornos"] > 0
        assert resumo["eventos_risco"] > 0

    def test_e_deterministica(self, settings):
        """Sem massa reproduzível não da para testar idempotência."""
        primeira = gerar_massa(settings, qtd_clientes=30, dias=3)
        segunda = gerar_massa(settings, qtd_clientes=30, dias=3)

        assert primeira == segunda


class TestBronze:
    def test_ingere_e_adiciona_os_metadados(self, spark, settings):
        gerar_massa(settings, qtd_clientes=20, dias=2)

        ingerir(spark, "clientes", "clientes", settings, formato="json")
        bronze = _tabela(spark, settings, "bronze", "clientes")

        assert bronze.count() > 0
        for coluna in ["arquivo_origem", "data_ingestao", "timestamp_ingestao",
                       "batch_id", "hash_linha", "schema_version"]:
            assert coluna in bronze.columns

        assert bronze.filter(F.col("batch_id") == settings.batch_id).count() == bronze.count()

    def test_nao_reingere_arquivo_ja_processado(self, spark, settings):
        """Idempotência da Bronze: reprocessar não duplica linha."""
        gerar_massa(settings, qtd_clientes=20, dias=2)

        ingerir(spark, "clientes", "clientes", settings, formato="json")
        depois_da_primeira = _tabela(spark, settings, "bronze", "clientes").count()

        segunda = ingerir(spark, "clientes", "clientes", settings, formato="json")

        assert segunda is None
        assert _tabela(spark, settings, "bronze", "clientes").count() == depois_da_primeira

    def test_registra_os_arquivos_ja_ingeridos(self, spark, settings):
        gerar_massa(settings, qtd_clientes=20, dias=3)

        ingerir(spark, "transacoes", "transacoes", settings, formato="csv")
        arquivos = arquivos_ja_ingeridos(spark, "transacoes", settings)

        assert len(arquivos) == 3

    def test_absorve_arquivo_com_coluna_nova(self, spark, settings):
        """O último arquivo da massa tem a coluna dispositivo.

        A carga precisa aceitar a coluna nova sem quebrar e sem perder as linhas
        antigas, que ficam com nulo nela.
        """
        gerar_massa(settings, qtd_clientes=20, dias=3)

        ingerir(spark, "transacoes", "transacoes", settings, formato="csv")
        bronze = _tabela(spark, settings, "bronze", "transacoes")

        assert "dispositivo" in bronze.columns
        assert bronze.filter(F.col("dispositivo").isNotNull()).count() > 0
        assert bronze.filter(F.col("dispositivo").isNull()).count() > 0

    def test_le_tudo_como_string(self, spark, settings):
        """Bronze preserva o byte original; a conversão e responsabilidade da Prata."""
        gerar_massa(settings, qtd_clientes=20, dias=2)

        ingerir(spark, "transacoes", "transacoes", settings, formato="csv")
        tipos = dict(_tabela(spark, settings, "bronze", "transacoes").dtypes)

        assert tipos["valor"] == "string"
        assert tipos["id_transacao"] == "string"


class TestPipelineCompleto:
    def test_executa_as_tres_camadas(self, spark, ambiente_executado):
        settings, resumo = ambiente_executado

        assert resumo["etapas_com_falha"] == 0
        assert resumo["linhas_gravadas"] > 0

        for nome in ["dim_cliente", "dim_conta", "dim_cartao", "fato_transacao",
                     "fato_estorno", "fato_evento_risco"]:
            assert _tabela(spark, settings, "silver", nome).count() > 0

        for nome in ["gold_fato_transacao", "gold_cliente_mes", "gold_indicadores_risco",
                     "gold_features_cliente", "gold_dim_estabelecimento",
                     "gold_dim_cliente", "gold_dim_conta", "gold_dim_cartao"]:
            assert _tabela(spark, settings, "gold", nome).count() > 0

    def test_dimensao_ouro_tem_uma_linha_por_chave(self, spark, ambiente_executado):
        """As dimensões Ouro são a versão corrente, sem histórico.

        Se o filtro de versão vigente escapar, cada chave aparece uma vez por
        versão e qualquer contagem distinta que use estas tabelas infla.
        """
        settings, _ = ambiente_executado

        for nome, chave in [("gold_dim_cliente", "id_cliente"),
                            ("gold_dim_conta", "id_conta"),
                            ("gold_dim_cartao", "id_cartao")]:
            dim = _tabela(spark, settings, "gold", nome)
            assert dim.count() == dim.select(chave).distinct().count(), nome

    def test_dimensao_ouro_nao_expoe_coluna_tecnica_do_scd(self, spark, ambiente_executado):
        """Quem consome BI não deve precisar saber o que é dw_versao_ativa."""
        settings, _ = ambiente_executado

        for nome in ["gold_dim_cliente", "gold_dim_conta", "gold_dim_cartao"]:
            colunas = _tabela(spark, settings, "gold", nome).columns
            assert not [c for c in colunas if c.startswith("dw_")], nome

    def test_registra_metricas_de_todas_as_etapas(self, spark, ambiente_executado):
        settings, _ = ambiente_executado
        metricas = _tabela(spark, settings, "observabilidade", "execucao_pipeline")

        assert metricas.count() > 0
        assert metricas.filter(F.col("status") == "FALHA").count() == 0
        assert {r["camada"] for r in metricas.select("camada").distinct().collect()} == {
            "bronze", "silver", "gold"
        }
        assert metricas.filter(F.col("batch_id") == settings.batch_id).count() == metricas.count()

    def test_manda_os_registros_invalidos_para_quarentena(self, spark, ambiente_executado):
        """A massa injeta CPF nulo, CPF curto, UF inválida, renda negativa,
        valor zero, valor negativo e três tipos de órfão."""
        settings, _ = ambiente_executado
        quarentena = _tabela(spark, settings, "quarentena", "registros_rejeitados")

        assert quarentena.count() > 0

        motivos = {
            linha["motivo"]
            for linha in quarentena.select(
                F.explode("motivos_rejeicao").alias("motivo")
            ).distinct().collect()
        }

        assert "cliente_cpf_obrigatorio" in motivos
        assert "cliente_uf_valida" in motivos
        assert "cliente_renda_nao_negativa" in motivos
        assert "transacao_valor_positivo" in motivos
        # Vínculo quebrado cai na mesma quarentena, com motivo próprio.
        assert "cartao_sem_conta_valida" in motivos
        assert "transacao_sem_cartao_valido" in motivos
        assert "estorno_sem_transacao_valida" in motivos

    def test_nao_deixa_transacao_duplicada_na_prata(self, spark, ambiente_executado):
        """A massa repete cinco transações do dia 1 no arquivo do dia 2."""
        settings, _ = ambiente_executado
        fato = _tabela(spark, settings, "silver", "fato_transacao")

        assert fato.count() == fato.select("id_transacao").distinct().count()

    def test_dimensao_nao_tem_duas_versoes_ativas_para_a_mesma_chave(self, spark, ambiente_executado):
        """Invariante do SCD Tipo 2: no máximo uma versão corrente por chave."""
        settings, _ = ambiente_executado

        for nome, chave in [("dim_cliente", "id_cliente"), ("dim_conta", "id_conta"),
                            ("dim_cartao", "id_cartao")]:
            ativas = (
                _tabela(spark, settings, "silver", nome)
                .filter(F.col("dw_versao_ativa"))
                .groupBy(chave)
                .count()
                .filter(F.col("count") > 1)
            )
            assert ativas.count() == 0, f"{nome} tem chave com mais de uma versao ativa"

    def test_dimensao_nao_tem_intervalo_invertido(self, spark, ambiente_executado):
        settings, _ = ambiente_executado

        for nome in ["dim_cliente", "dim_conta", "dim_cartao"]:
            invertidos = _tabela(spark, settings, "silver", nome).filter(
                F.col("dw_fim_vigencia").isNotNull()
                & (F.col("dw_fim_vigencia") <= F.col("dw_inicio_vigencia"))
            )
            assert invertidos.count() == 0, f"{nome} tem intervalo de vigencia invertido"

    def test_dimensao_registra_versoes_historicas(self, spark, ambiente_executado):
        """A massa injeta atualizacao cadastral, então precisa existir historia."""
        settings, _ = ambiente_executado
        dim = _tabela(spark, settings, "silver", "dim_cliente")

        assert dim.filter(~F.col("dw_versao_ativa")).count() > 0
        assert dim.filter(F.col("dw_fim_vigencia").isNotNull()).count() > 0


class TestRegrasDeNegocioNaMassaCompleta:
    def test_soma_do_liquido_bate_com_bruto_menos_estornado(self, spark, ambiente_executado):
        settings, _ = ambiente_executado
        totais = _tabela(spark, settings, "gold", "gold_fato_transacao").agg(
            F.sum("valor_bruto").alias("bruto"),
            F.sum("valor_estornado").alias("estornado"),
            F.sum("valor_liquido").alias("liquido"),
        ).collect()[0]

        assert totais["liquido"] == totais["bruto"] - totais["estornado"]

    def test_grao_do_fato_ouro_e_uma_linha_por_transacao(self, spark, ambiente_executado):
        """Se algum join multiplicar, a soma do bruto deixa de conciliar."""
        settings, _ = ambiente_executado
        gold = _tabela(spark, settings, "gold", "gold_fato_transacao")
        silver = _tabela(spark, settings, "silver", "fato_transacao")

        assert gold.count() == gold.select("id_transacao").distinct().count()
        assert gold.count() == silver.count()

    def test_features_tem_uma_linha_por_cliente(self, spark, ambiente_executado):
        settings, _ = ambiente_executado
        features = _tabela(spark, settings, "gold", "gold_features_cliente")

        assert features.count() == features.select("id_cliente").distinct().count()

    def test_features_usam_a_data_de_referencia(self, spark, ambiente_executado):
        """Feature calculada com o relógio da máquina não seria reproduzível."""
        settings, _ = ambiente_executado
        features = _tabela(spark, settings, "gold", "gold_features_cliente")
        datas = {
            linha["f_data_referencia"]
            for linha in features.select("f_data_referencia").distinct().collect()
        }

        assert datas == {settings.data_referencia}

    def test_transacao_de_cartao_cancelado_fica_no_fato_mas_fora_da_metrica(
        self, spark, ambiente_executado
    ):
        """A massa cancela parte dos cartões depois de haver movimento neles."""
        settings, _ = ambiente_executado
        fato = _tabela(spark, settings, "gold", "gold_fato_transacao")

        nao_elegiveis = fato.filter(~F.col("elegivel_metrica"))
        assert nao_elegiveis.count() > 0, "a massa deveria produzir transacao nao elegivel"

        # Elas continuam no fato; o que muda é a elegibilidade.
        assert fato.count() > nao_elegiveis.count()


class TestIdempotenciaDoPipeline:
    """Estes casos controlam quantas vezes o pipeline roda, então usam ambiente
    próprio em vez da fixture compartilhada."""

    @pytest.fixture
    def ambiente(self, spark, settings):
        gerar_massa(settings, qtd_clientes=QTD_CLIENTES, dias=DIAS)
        return settings

    def _contagens(self, spark, settings):
        return {
            f"{camada}.{nome}": _tabela(spark, settings, camada, nome).count()
            for camada, nome in [
                ("silver", "dim_cliente"), ("silver", "dim_conta"), ("silver", "dim_cartao"),
                ("silver", "fato_transacao"), ("silver", "fato_estorno"),
                ("gold", "gold_fato_transacao"), ("gold", "gold_cliente_mes"),
                ("quarentena", "registros_rejeitados"),
            ]
        }

    def test_coluna_nova_na_bronze_nao_altera_o_schema_da_prata(self, spark, settings):
        """Evolução de schema chegando com a Prata já materializada.

        A Bronze absorve a coluna nova, como deve. A Prata não muda, porque a
        tipagem faz select explícito das colunas do modelo: campo que a origem
        acrescenta só entra na Prata quando alguem decidir mapea-lo.

        Isso é o que mantém o MERGE dos fatos estável. Um pipeline que
        propagasse a coluna automaticamente exigiria autoMerge de schema no
        MERGE e mudaria o contrato da Prata sem ninguem revisar.
        """
        import csv
        from pathlib import Path

        gerar_massa(settings, qtd_clientes=20, dias=2)
        executar(settings, spark)

        prata_antes = set(_tabela(spark, settings, "silver", "fato_transacao").columns)
        bronze_antes = set(_tabela(spark, settings, "bronze", "transacoes").columns)
        assert "canal_origem" not in bronze_antes

        # Arquivo novo com uma coluna que nunca apareceu na origem.
        transacoes = Path(settings.landing("transacoes"))
        modelo = sorted(transacoes.glob("*.csv"))[0]
        with modelo.open(encoding="utf-8") as arquivo:
            linhas = list(csv.DictReader(arquivo))

        novo = transacoes / "transacoes_2026-04-01.csv"
        colunas = list(linhas[0].keys()) + ["canal_origem"]
        with novo.open("w", newline="", encoding="utf-8") as arquivo:
            escritor = csv.DictWriter(arquivo, fieldnames=colunas)
            escritor.writeheader()
            for i, linha in enumerate(linhas[:20]):
                escritor.writerow({
                    **linha,
                    "id_transacao": 800000 + i,
                    "data_transacao": "2026-04-01 12:00:00",
                    "canal_origem": "PARCEIRO",
                })

        executar(settings, spark)

        bronze_depois = _tabela(spark, settings, "bronze", "transacoes")
        prata_depois = _tabela(spark, settings, "silver", "fato_transacao")

        # A Bronze absorveu o campo novo sem quebrar e sem perder o histórico.
        assert "canal_origem" in bronze_depois.columns
        assert bronze_depois.filter(F.col("canal_origem").isNull()).count() > 0
        assert bronze_depois.filter(F.col("canal_origem") == "PARCEIRO").count() == 20

        # A Prata manteve o contrato e absorveu as linhas novas.
        assert set(prata_depois.columns) == prata_antes
        assert prata_depois.count() > _tabela(spark, settings, "bronze", "transacoes").count() - 20

    def test_rodar_duas_vezes_produz_o_mesmo_resultado(self, spark, ambiente):
        """O requisito central: reprocessar não perde nem duplica dado."""
        executar(ambiente, spark)
        primeira = self._contagens(spark, ambiente)

        executar(ambiente, spark)
        segunda = self._contagens(spark, ambiente)

        assert primeira == segunda

    def test_valor_liquido_nao_muda_no_reprocessamento(self, spark, ambiente):
        """Contagem igual não basta: o valor agregado também precisa bater."""
        def total():
            return _tabela(spark, ambiente, "gold", "gold_fato_transacao").agg(
                F.sum("valor_liquido").alias("t")
            ).collect()[0]["t"]

        executar(ambiente, spark)
        primeira = total()

        executar(ambiente, spark)
        assert total() == primeira
