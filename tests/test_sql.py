"""Validação das consultas SQL entregues.

Consulta que nunca rodou e documentacao, não entrega. Estes testes leem os
arquivos de ``sql/``, montam views vazias com o schema real de cada tabela e
submetem cada consulta ao analisador do Spark.

O que isso pega: erro de sintaxe, coluna inexistente, função que o engine não
tem, agregação mal formada. O que não pega: resultado errado com dado certo.
Para isso existem os testes das camadas.

As views são vazias de propósito. O objetivo e validar o plano, e criar dado
tornaria o teste lento sem cobrir nada a mais.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

RAIZ_SQL = Path(__file__).resolve().parent.parent / "sql"

# Schemas das tabelas que as consultas referenciam, iguais aos que o pipeline
# materializa. Divergência aqui é defeito no teste, não na consulta.
SCHEMAS = {
    "gold_fato_transacao": """
        id_transacao long, id_cartao long, id_conta long, id_cliente long,
        data_transacao timestamp, data_particao date, ano int, mes int, ano_mes string,
        valor_bruto decimal(18,2), valor_estornado decimal(18,2), valor_liquido decimal(18,2),
        foi_estornada boolean, estorno_total boolean, qtd_estornos long, motivo_estorno string,
        mcc int, estabelecimento string, canal string, pais string, moeda string,
        dispositivo string, tipo_cartao string, status_cartao_na_data string,
        limite_na_data decimal(18,2), tipo_conta string, status_conta_na_data string,
        segmento_na_data string, cidade_na_data string, estado_na_data string,
        renda_na_data decimal(18,2), elegivel_metrica boolean
    """,
    "dim_cliente": """
        id_cliente long, cpf string, nome string, cidade string, estado string,
        renda decimal(18,2), segmento string, data_atualizacao timestamp, operacao string,
        arquivo_origem string, batch_id string, dw_hash_atributos string,
        dw_inicio_vigencia timestamp, dw_fim_vigencia timestamp, dw_versao_ativa boolean,
        dw_excluido boolean, dw_batch_id string, dw_atualizado_em timestamp
    """,
    "fato_transacao": """
        id_transacao long, id_cartao long, data_transacao timestamp, valor decimal(18,2),
        mcc int, estabelecimento string, canal string, pais string, moeda string,
        dispositivo string, data_particao date, arquivo_origem string, batch_id string,
        timestamp_ingestao timestamp
    """,
    "stg_cliente": """
        id_cliente long, cpf string, nome string, cidade string, estado string,
        renda decimal(18,2), segmento string, data_atualizacao timestamp, operacao string
    """,
    "stg_transacao": """
        id_transacao long, id_cartao long, data_transacao timestamp, valor decimal(18,2),
        mcc int, estabelecimento string, canal string, pais string, moeda string,
        dispositivo string, data_particao date, arquivo_origem string, batch_id string,
        timestamp_ingestao timestamp
    """,
    "execucao_pipeline": """
        batch_id string, data_referencia string, camada string, etapa string,
        entidade string, linhas_lidas long, linhas_gravadas long, linhas_rejeitadas long,
        duracao_segundos int, status string, detalhe_erro string,
        iniciado_em timestamp, finalizado_em timestamp
    """,
    "registros_rejeitados": """
        entidade string, camada_origem string, batch_id string, data_referencia string,
        quarentenado_em timestamp, motivos_rejeicao array<string>,
        registro_original string, hash_rejeicao string
    """,
}


def _consultas_do_arquivo(caminho: Path) -> list[tuple[int, str]]:
    """Separa as instruções de um arquivo, ignorando comentarios e linhas vazias.

    Retorna (linha_inicial, sql) para que a falha aponte o lugar no arquivo.
    """
    texto = caminho.read_text(encoding="utf-8")
    instruções: list[tuple[int, str]] = []
    acumulado: list[str] = []
    linha_inicial = 1

    for número, linha in enumerate(texto.splitlines(), start=1):
        sem_comentario = re.sub(r"--.*$", "", linha)

        if not acumulado and sem_comentario.strip():
            linha_inicial = número

        acumulado.append(sem_comentario)

        if ";" in sem_comentario:
            sql = "\n".join(acumulado).strip().rstrip(";").strip()
            if sql:
                instruções.append((linha_inicial, sql))
            acumulado = []

    resto = "\n".join(acumulado).strip()
    if resto:
        instruções.append((linha_inicial, resto))

    return instruções


def _casos():
    """Uma entrada por consulta, identificada por arquivo e linha."""
    casos = []
    for arquivo in sorted(RAIZ_SQL.glob("*.sql")):
        for linha, sql in _consultas_do_arquivo(arquivo):
            casos.append(pytest.param(sql, id=f"{arquivo.name}:{linha}"))
    return casos


# Alvos de MERGE precisam ser tabela Delta gerenciada: view temporaria não
# aceita a operação, e o erro que o Spark devolve nesse caso não distingue
# "consulta inválida" de "alvo inválido".
ALVOS_DE_MERGE = ["dim_cliente", "fato_transacao"]


@pytest.fixture(scope="module")
def catalogo(spark, tmp_path_factory):
    """Monta o catalogo que as consultas esperam encontrar.

    Alvos de MERGE viram tabela Delta gerenciada; o resto vira view temporaria,
    que é mais barato e suficiente para analisar um SELECT.
    """
    destino = tmp_path_factory.mktemp("sql-warehouse")
    spark.sql("CREATE DATABASE IF NOT EXISTS validacao_sql")
    spark.sql("USE validacao_sql")

    for nome, schema in SCHEMAS.items():
        if nome in ALVOS_DE_MERGE:
            caminho = str(destino / nome).replace("\\", "/")
            # DDL explícito em vez de saveAsTable com overwrite: dependendo da
            # versão do Delta, o overwrite tenta truncar a tabela e falha com
            # "does not support truncate in batch mode". O CREATE não tem essa
            # ambiguidade e o schema fica declarado no próprio comando.
            spark.sql(f"DROP TABLE IF EXISTS {nome}")
            spark.sql(
                f"CREATE TABLE {nome} ({schema}) USING DELTA LOCATION '{caminho}'"
            )
        else:
            spark.createDataFrame([], schema).createOrReplaceTempView(nome)

    # A Prata e referenciada com prefixo em algumas consultas.
    spark.sql("CREATE OR REPLACE TEMP VIEW silver_cliente AS SELECT * FROM dim_cliente")

    yield spark

    spark.sql("DROP DATABASE IF EXISTS validacao_sql CASCADE")
    spark.sql("USE default")


def test_encontrou_consultas():
    """Guarda contra o parser silenciosamente não achar nada."""
    assert len(_casos()) >= 10


@pytest.mark.parametrize("sql", _casos())
def test_consulta_e_valida(catalogo, sql):
    """Submete a consulta ao analisador do Spark.

    ``EXPLAIN`` faz parse e análise sem executar: válida sintaxe, existência de
    coluna e resolução de função, que é tudo o que dá para verificar sem dado.

    MERGE não aceita EXPLAIN em todas as versões do Delta, então ele e executado
    contra as tabelas vazias. Sem linha nenhuma, executar custa o mesmo que
    planejar e válida o comando inteiro, inclusive a clausula de ação.
    """
    if sql.lstrip().upper().startswith(("MERGE", "WITH")) and " MERGE INTO " in f" {sql.upper()} ":
        catalogo.sql(sql)
        return

    plano = catalogo.sql(f"EXPLAIN {sql}").collect()[0][0]

    # O Spark devolve o erro dentro do plano em vez de lancar exceção.
    assert "Error" not in plano and "Exception" not in plano, plano[:600]
