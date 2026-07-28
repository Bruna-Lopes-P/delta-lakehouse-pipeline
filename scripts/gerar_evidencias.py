"""Gera as evidencias de execução do pipeline.

Roda o pipeline sobre a massa sintética e grava em ``evidencias/`` o que outra
pessoa precisaria para conferir o resultado sem executar nada: log estruturado,
contagem por tabela, métricas por etapa, quarentena por motivo e as invariantes
verificadas.

Uso:
    python -m scripts.gerar_evidencias
"""

from __future__ import annotations

import shutil
import sys
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from src.config.settings import Settings
from src.datagen.generator import gerar_massa
from src.pipeline.run import executar
from src.utils.logging import configurar_logs
from src.utils.spark import ler_delta

RAIZ = Path(__file__).resolve().parent.parent
DESTINO = RAIZ / "evidencias"

TABELAS = [
    ("bronze", "clientes"), ("bronze", "contas"), ("bronze", "cartoes"),
    ("bronze", "transacoes"), ("bronze", "estornos"), ("bronze", "eventos_risco"),
    ("silver", "dim_cliente"), ("silver", "dim_conta"), ("silver", "dim_cartao"),
    ("silver", "fato_transacao"), ("silver", "fato_estorno"), ("silver", "fato_evento_risco"),
    ("gold", "gold_fato_transacao"), ("gold", "gold_cliente_mes"),
    ("gold", "gold_indicadores_risco"), ("gold", "gold_features_cliente"),
    ("gold", "gold_dim_estabelecimento"), ("gold", "gold_dim_cliente"),
    ("gold", "gold_dim_conta"), ("gold", "gold_dim_cartao"),
    ("quarentena", "registros_rejeitados"),
    ("observabilidade", "execucao_pipeline"),
]


def _sessao_local():
    """Sessão dimensionada para as duas execuções completas deste script.

    ``obter_sessao`` não fixa memória de propósito: em Databricks quem dimensiona
    é o cluster, e cravar valor no código brigaria com isso. Aqui o script sabe
    que vai rodar o pipeline inteiro duas vezes numa JVM so, acumulando metadados
    de alguns milhares de stages, e o heap padrão de 1 GB não aguenta. O sintoma
    da falta é enganoso: a JVM morre e o erro que aparece é de socket.
    """
    import os
    import sys

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    if sys.platform == "win32":
        hadoop_home = os.environ.get("HADOOP_HOME") or r"C:\hadoop"
        hadoop_bin = os.path.join(hadoop_home, "bin")
        if os.path.exists(os.path.join(hadoop_bin, "winutils.exe")):
            os.environ["HADOOP_HOME"] = hadoop_home
            if hadoop_bin.lower() not in os.environ.get("PATH", "").lower():
                os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

    from pyspark.sql import SparkSession

    construtor = (
        SparkSession.builder.appName("novarota-evidencias")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )

    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(construtor).getOrCreate()


def _escrever(nome: str, conteudo: str) -> None:
    caminho = DESTINO / nome
    caminho.write_text(conteudo, encoding="utf-8")
    print(f"gravado: {caminho.relative_to(RAIZ)}")


def _tabela_markdown(cabecalho: list[str], linhas: list[list[str]]) -> str:
    largura = [
        max(len(str(c)), *(len(str(linha[i])) for linha in linhas)) if linhas else len(str(c))
        for i, c in enumerate(cabecalho)
    ]
    sep = "|" + "|".join("-" * (w + 2) for w in largura) + "|"
    cab = "| " + " | ".join(str(c).ljust(largura[i]) for i, c in enumerate(cabecalho)) + " |"
    corpo = [
        "| " + " | ".join(str(v).ljust(largura[i]) for i, v in enumerate(linha)) + " |"
        for linha in linhas
    ]
    return "\n".join([cab, sep, *corpo])


def main() -> None:
    if DESTINO.exists():
        shutil.rmtree(DESTINO)
    DESTINO.mkdir(parents=True)

    configurar_logs("INFO")

    settings = Settings(
        ambiente="dev",
        raiz_dados=str(DESTINO / "lakehouse").replace("\\", "/"),
        raiz_landing=str(DESTINO / "landing").replace("\\", "/"),
        data_referencia=date(2026, 3, 31),
        batch_id="evidencia01",
        modo_execucao="completa",
        usar_unity_catalog=False,
    )

    spark = _sessao_local()
    spark.sparkContext.setLogLevel("ERROR")

    print("gerando massa sintética...")
    resumo_massa = gerar_massa(settings, qtd_clientes=200, dias=5)

    print("executando o pipeline...")
    resumo_execucao = executar(settings, spark)

    print("executando de novo, para evidenciar idempotência...")
    settings_segunda = Settings(
        ambiente=settings.ambiente,
        raiz_dados=settings.raiz_dados,
        raiz_landing=settings.raiz_landing,
        data_referencia=settings.data_referencia,
        batch_id="evidencia02",
        modo_execucao="completa",
        usar_unity_catalog=False,
    )

    contagens_primeira = {
        f"{camada}.{nome}": ler_delta(spark, settings.tabela(camada, nome), settings).count()
        for camada, nome in TABELAS
    }

    executar(settings_segunda, spark)

    contagens_segunda = {
        f"{camada}.{nome}": ler_delta(spark, settings.tabela(camada, nome), settings).count()
        for camada, nome in TABELAS
    }

    # --- contagem por tabela e prova de idempotência -------------------------
    #
    # A tabela de observabilidade cresce por design: ela registra que o pipeline
    # executou, e a segunda execução é um evento novo. Idempotência é uma
    # propriedade do dado processado, não do log de quem processou.
    CRESCE_POR_DESIGN = {"observabilidade.execucao_pipeline"}

    linhas = []
    for chave in contagens_primeira:
        primeira, segunda = contagens_primeira[chave], contagens_segunda[chave]
        if chave in CRESCE_POR_DESIGN:
            situacao = "cresce por design" if segunda > primeira else "ATENÇÃO não cresceu"
        else:
            situacao = "igual" if primeira == segunda else "DIVERGIU"
        linhas.append([chave, primeira, segunda, situacao])

    divergencias = [linha for linha in linhas if linha[3].startswith(("DIVERGIU", "ATENCAO"))]

    _escrever(
        "01_contagem_por_tabela.md",
        "# Contagem por tabela\n\n"
        "Duas execuções completas sobre a mesma massa. A coluna de comparacao e a\n"
        "prova de idempotência: reprocessar não perde nem duplica.\n\n"
        "A única tabela que cresce é a de observabilidade, e ela deve crescer: registra\n"
        "que o pipeline executou, e a segunda execução é um evento novo. Idempotência é\n"
        "propriedade do dado processado, não do log de quem processou.\n\n"
        + _tabela_markdown(
            ["tabela", "1a execução", "2a execução", "comparação"], linhas
        )
        + f"\n\nDivergencias inesperadas: {len(divergencias)}\n",
    )

    # --- métricas por etapa --------------------------------------------------
    metricas = ler_delta(
        spark, settings.tabela("observabilidade", "execucao_pipeline"), settings
    ).orderBy("iniciado_em")

    linhas_metricas = [
        [r["batch_id"], r["camada"], r["etapa"], r["entidade"],
         r["linhas_lidas"], r["linhas_gravadas"], r["linhas_rejeitadas"],
         r["duracao_segundos"], r["status"]]
        for r in metricas.collect()
    ]
    _escrever(
        "02_metricas_por_etapa.md",
        "# Métricas por etapa\n\n"
        "Uma linha por etapa executada, gravada em Delta pelo proprio pipeline.\n\n"
        + _tabela_markdown(
            ["batch", "camada", "etapa", "entidade", "lidas", "gravadas",
             "rejeitadas", "segundos", "status"],
            linhas_metricas,
        )
        + "\n",
    )

    # --- quarentena por motivo ----------------------------------------------
    quarentena = ler_delta(
        spark, settings.tabela("quarentena", "registros_rejeitados"), settings
    )
    por_motivo = (
        quarentena.select("entidade", F.explode("motivos_rejeicao").alias("motivo"))
        .groupBy("entidade", "motivo")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    exemplo = quarentena.limit(3).collect()

    _escrever(
        "03_quarentena.md",
        "# Quarentena\n\n"
        f"Total de registros rejeitados: {quarentena.count()}\n\n"
        "Todos os defeitos abaixo foram injetados de propósito na massa sintética.\n\n"
        + _tabela_markdown(
            ["entidade", "motivo", "quantidade"],
            [[r["entidade"], r["motivo"], r["count"]] for r in por_motivo],
        )
        + "\n\n## Exemplo de registro preservado\n\n"
        + "\n".join(
            f"- **{r['entidade']}** | motivos: `{', '.join(r['motivos_rejeicao'])}`\n"
            f"  ```json\n  {r['registro_original'][:300]}\n  ```"
            for r in exemplo
        )
        + "\n",
    )

    # --- invariantes ---------------------------------------------------------
    verificacoes = []

    for nome, chave in [("dim_cliente", "id_cliente"), ("dim_conta", "id_conta"),
                        ("dim_cartao", "id_cartao")]:
        dim = ler_delta(spark, settings.tabela("silver", nome), settings)
        duplicadas = (
            dim.filter(F.col("dw_versao_ativa")).groupBy(chave).count()
            .filter(F.col("count") > 1).count()
        )
        invertidos = dim.filter(
            F.col("dw_fim_vigencia").isNotNull()
            & (F.col("dw_fim_vigencia") <= F.col("dw_inicio_vigencia"))
        ).count()
        historico = dim.filter(~F.col("dw_versao_ativa")).count()

        verificacoes.append([f"{nome}: chaves com mais de uma versão ativa", duplicadas, 0,
                             "ok" if duplicadas == 0 else "FALHOU"])
        verificacoes.append([f"{nome}: intervalos de vigência invertidos ou vazios", invertidos, 0,
                             "ok" if invertidos == 0 else "FALHOU"])
        verificacoes.append([f"{nome}: versões históricas preservadas", historico, "> 0",
                             "ok" if historico > 0 else "FALHOU"])

    fato_silver = ler_delta(spark, settings.tabela("silver", "fato_transacao"), settings)
    duplicadas_fato = fato_silver.count() - fato_silver.select("id_transacao").distinct().count()
    verificacoes.append(["fato_transacao: transações duplicadas", duplicadas_fato, 0,
                         "ok" if duplicadas_fato == 0 else "FALHOU"])

    fato_gold = ler_delta(spark, settings.tabela("gold", "gold_fato_transacao"), settings)
    totais = fato_gold.agg(
        F.sum("valor_bruto").alias("bruto"),
        F.sum("valor_estornado").alias("estornado"),
        F.sum("valor_liquido").alias("liquido"),
    ).collect()[0]
    bate = totais["liquido"] == totais["bruto"] - totais["estornado"]
    verificacoes.append(["gold: líquido == bruto - estornado", str(bate), "True",
                         "ok" if bate else "FALHOU"])

    grao = fato_gold.count() == fato_gold.select("id_transacao").distinct().count()
    verificacoes.append(["gold: grão de uma linha por transação", str(grao), "True",
                         "ok" if grao else "FALHOU"])

    conciliacao = fato_gold.count() == fato_silver.count()
    verificacoes.append(["gold concilia com a Prata em contagem", str(conciliacao), "True",
                         "ok" if conciliacao else "FALHOU"])

    nao_elegiveis = fato_gold.filter(~F.col("elegivel_metrica")).count()
    verificacoes.append(["gold: transações de cartão cancelado preservadas", nao_elegiveis, "> 0",
                         "ok" if nao_elegiveis > 0 else "FALHOU"])

    falhas = [v for v in verificacoes if v[3] == "FALHOU"]

    _escrever(
        "04_invariantes.md",
        "# Invariantes verificadas\n\n"
        "Checagens sobre o resultado das duas execuções. Qualquer FALHOU aqui e\n"
        "defeito de carga, não característica do dado.\n\n"
        + _tabela_markdown(
            ["verificação", "obtido", "esperado", "resultado"], verificacoes
        )
        + f"\n\nFalhas: {len(falhas)}\n",
    )

    # --- totais do negocio ---------------------------------------------------
    _escrever(
        "05_resumo.md",
        "# Resumo da execução\n\n"
        "## Massa sintética gerada\n\n"
        + _tabela_markdown(
            ["origem", "linhas"], [[k, v] for k, v in resumo_massa.items()]
        )
        + "\n\n## Totais da execução\n\n"
        + _tabela_markdown(
            ["métrica", "valor"], [[k, v] for k, v in resumo_execucao.items()]
        )
        + "\n\n## Valores do negócio na camada Ouro\n\n"
        + _tabela_markdown(
            ["métrica", "valor"],
            [
                ["valor bruto total", f"{totais['bruto']:,.2f}"],
                ["valor estornado total", f"{totais['estornado']:,.2f}"],
                ["valor líquido total", f"{totais['liquido']:,.2f}"],
                ["transações no fato", fato_gold.count()],
                ["transações não elegíveis (cartão ou conta inativa na data)", nao_elegiveis],
            ],
        )
        + "\n",
    )

    _escrever(
        "00_como_foi_gerado.md",
        "# Como estas evidencias foram geradas\n\n"
        "```bash\n"
        "python -m scripts.gerar_evidencias\n"
        "```\n\n"
        "O script gera a massa sintética, executa o pipeline duas vezes sobre ela e\n"
        "grava os arquivos deste diretório. A segunda execução existe para evidenciar\n"
        "idempotência: as contagens em `01_contagem_por_tabela.md` sao idênticas.\n\n"
        "A massa usa semente fixa, entao rodar de novo produz os mesmos números.\n\n"
        "## Parametros\n\n"
        + _tabela_markdown(
            ["parâmetro", "valor"],
            [
                ["ambiente", settings.ambiente],
                ["data de referência", settings.data_referencia.isoformat()],
                ["batch da 1a execução", settings.batch_id],
                ["batch da 2a execução", settings_segunda.batch_id],
                ["clientes na massa", 200],
                ["dias de transação", 5],
            ],
        )
        + "\n\n## Arquivos\n\n"
        "| arquivo | conteúdo |\n"
        "|---|---|\n"
        "| `01_contagem_por_tabela.md` | linhas por tabela nas duas execuções |\n"
        "| `02_metricas_por_etapa.md` | lidas, gravadas, rejeitadas e duração por etapa |\n"
        "| `03_quarentena.md` | rejeições por motivo e exemplo preservado |\n"
        "| `04_invariantes.md` | checagens sobre o resultado |\n"
        "| `05_resumo.md` | totais da massa, da execução e do negócio |\n",
    )

    # O lakehouse gerado não vai para o repositorio; só os relatórios.
    shutil.rmtree(DESTINO / "lakehouse", ignore_errors=True)
    shutil.rmtree(DESTINO / "landing", ignore_errors=True)

    print()
    if falhas or divergencias:
        print(f"ATENCAO: {len(falhas)} invariante(s) e {len(divergencias)} divergência(s)")
        sys.exit(1)

    print("evidencias geradas sem divergência")


if __name__ == "__main__":
    main()
