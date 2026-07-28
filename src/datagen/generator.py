"""Geração da massa de dados sintética.

Vem com defeito de propósito: CPF nulo e inválido, renda negativa, duplicata
exata, atualizacao cadastral, conta encerrada, cartão cancelado, cartão e
transação órfãos, valor zero e negativo, transação repetida entre arquivos,
arquivo com coluna nova e estorno sem transação.

Semente fixa: duas execuções produzem os mesmos arquivos.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from src.config.settings import Settings
from src.utils.logging import obter_logger

logger = obter_logger("datagen")

SEMENTE = 42

CIDADES = [
    ("Porto Alegre", "RS"),
    ("Caxias do Sul", "RS"),
    ("Florianopolis", "SC"),
    ("Curitiba", "PR"),
    ("Londrina", "PR"),
    ("Sao Paulo", "SP"),
]
SEGMENTOS = ["VAREJO", "PREMIUM", "EMPRESARIAL", "AGRO"]
CANAIS = ["POS", "ECOMMERCE", "APP", "SAQUE"]
MCCS = [5411, 5541, 5812, 5912, 6011, 7011, 4121, 5999]
ESTABELECIMENTOS = {
    5411: ["Mercado Central", "Super Bom Preco", "Hiper Sul"],
    5541: ["Posto Ipiranga", "Posto BR"],
    5812: ["Restaurante do Ze", "Cantina Italia"],
    5912: ["Drogaria Sao Joao", "Farmacia Panvel"],
    6011: ["Saque ATM Banco24h"],
    7011: ["Hotel Serrano", "Pousada Vale"],
    4121: ["Uber", "99 Pop"],
    5999: ["Loja Variedades", "Mercado Livre"],
}


def _cpf_valido(indice: int) -> str:
    """CPF fictício com 11 dígitos. Não é um CPF real."""
    return f"{indice:011d}"


def _escrever_csv(caminho: Path, linhas: list[dict], colunas: list[str]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=colunas)
        escritor.writeheader()
        for linha in linhas:
            escritor.writerow(linha)
    logger.info(
        "arquivo gerado",
        extra={"caminho": str(caminho), "linhas": len(linhas)},
    )


def _escrever_jsonl(caminho: Path, linhas: list[dict]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8") as arquivo:
        for linha in linhas:
            arquivo.write(json.dumps(linha, ensure_ascii=False) + "\n")
    logger.info(
        "arquivo gerado",
        extra={"caminho": str(caminho), "linhas": len(linhas)},
    )


def gerar_clientes(qtd: int, base: date) -> list[dict]:
    """CDC de clientes, com duplicata, inválidos e atualizacao cadastral."""
    rng = random.Random(SEMENTE)
    registros: list[dict] = []

    for i in range(1, qtd + 1):
        cidade, uf = rng.choice(CIDADES)
        registros.append(
            {
                "id_cliente": i,
                "cpf": _cpf_valido(i),
                "nome": f"Cliente {i:04d}",
                "cidade": cidade,
                "estado": uf,
                "renda": round(rng.uniform(1800, 25000), 2),
                "segmento": rng.choice(SEGMENTOS),
                "data_atualizacao": (base - timedelta(days=120)).isoformat(),
                "operacao": "INSERT",
            }
        )

    # Atualizacao cadastral: 15% dos clientes mudam de segmento, cidade e renda.
    # Vira versão nova no SCD Tipo 2.
    for cliente in rng.sample(registros, k=max(1, qtd // 7)):
        cidade, uf = rng.choice(CIDADES)
        registros.append(
            {
                **cliente,
                "cidade": cidade,
                "estado": uf,
                "renda": round(cliente["renda"] * rng.uniform(1.1, 1.6), 2),
                "segmento": rng.choice(SEGMENTOS),
                "data_atualizacao": (base - timedelta(days=45)).isoformat(),
                "operacao": "UPDATE",
            }
        )

    # Duplicata exata: mesma linha repetida. Deve ser deduplicada sem virar versão.
    registros.append(dict(registros[0]))

    # CPF nulo: vai para quarentena.
    registros.append(
        {
            "id_cliente": 9001,
            "cpf": None,
            "nome": "Cliente Sem Documento",
            "cidade": "Porto Alegre",
            "estado": "RS",
            "renda": 4200.00,
            "segmento": "VAREJO",
            "data_atualizacao": (base - timedelta(days=30)).isoformat(),
            "operacao": "INSERT",
        }
    )

    # CPF com tamanho inválido: vai para quarentena.
    registros.append(
        {
            "id_cliente": 9002,
            "cpf": "123",
            "nome": "Cliente CPF Curto",
            "cidade": "Curitiba",
            "estado": "PR",
            "renda": 7800.00,
            "segmento": "PREMIUM",
            "data_atualizacao": (base - timedelta(days=30)).isoformat(),
            "operacao": "INSERT",
        }
    )

    # Renda negativa: vai para quarentena.
    registros.append(
        {
            "id_cliente": 9003,
            "cpf": _cpf_valido(9003),
            "nome": "Cliente Renda Negativa",
            "cidade": "Sao Paulo",
            "estado": "SP",
            "renda": -1500.00,
            "segmento": "VAREJO",
            "data_atualizacao": (base - timedelta(days=30)).isoformat(),
            "operacao": "INSERT",
        }
    )

    # UF inexistente: vai para quarentena.
    registros.append(
        {
            "id_cliente": 9004,
            "cpf": _cpf_valido(9004),
            "nome": "Cliente UF Invalida",
            "cidade": "Cidade Fantasma",
            "estado": "ZZ",
            "renda": 5100.00,
            "segmento": "VAREJO",
            "data_atualizacao": (base - timedelta(days=30)).isoformat(),
            "operacao": "INSERT",
        }
    )

    return registros


def gerar_contas(clientes: list[dict], base: date) -> list[dict]:
    """CDC de contas. Um cliente pode ter mais de uma conta."""
    rng = random.Random(SEMENTE + 1)
    ids_clientes = sorted({c["id_cliente"] for c in clientes if c["id_cliente"] < 9000})
    registros: list[dict] = []
    id_conta = 1

    for id_cliente in ids_clientes:
        for _ in range(rng.randint(1, 3)):
            registros.append(
                {
                    "id_conta": id_conta,
                    "id_cliente": id_cliente,
                    "tipo_conta": rng.choice(["CORRENTE", "POUPANCA"]),
                    "status_conta": "ATIVA",
                    "data_abertura": (base - timedelta(days=rng.randint(200, 1500))).isoformat(),
                    "data_atualizacao": (base - timedelta(days=120)).isoformat(),
                    "operacao": "INSERT",
                }
            )
            id_conta += 1

    # Encerramento: 10% das contas mudam de status.
    for conta in rng.sample(registros, k=max(1, len(registros) // 10)):
        registros.append(
            {
                **conta,
                "status_conta": "ENCERRADA",
                "data_atualizacao": (base - timedelta(days=40)).isoformat(),
                "operacao": "UPDATE",
            }
        )

    # Conta apontando para cliente inexistente: quebra de integridade referencial.
    registros.append(
        {
            "id_conta": 99001,
            "id_cliente": 888888,
            "tipo_conta": "CORRENTE",
            "status_conta": "ATIVA",
            "data_abertura": (base - timedelta(days=300)).isoformat(),
            "data_atualizacao": (base - timedelta(days=30)).isoformat(),
            "operacao": "INSERT",
        }
    )

    return registros


def gerar_cartoes(contas: list[dict], base: date) -> list[dict]:
    """CDC de cartões, com cancelamento, mudança de limite e cartão órfão."""
    rng = random.Random(SEMENTE + 2)
    ids_contas = sorted({c["id_conta"] for c in contas if c["id_conta"] < 90000})
    registros: list[dict] = []
    id_cartao = 1

    for id_conta in ids_contas:
        for _ in range(rng.randint(1, 2)):
            registros.append(
                {
                    "id_cartao": id_cartao,
                    "id_conta": id_conta,
                    "tipo_cartao": rng.choice(["CREDITO", "DEBITO", "MULTIPLO"]),
                    "limite": float(rng.randrange(500, 30000, 500)),
                    "status_cartao": "ATIVO",
                    "data_atualizacao": (base - timedelta(days=120)).isoformat(),
                    "operacao": "INSERT",
                }
            )
            id_cartao += 1

    # Aumento de limite: gera versão nova, o cartão continua ativo.
    for cartao in rng.sample(registros, k=max(1, len(registros) // 5)):
        registros.append(
            {
                **cartao,
                "limite": cartao["limite"] * 2,
                "data_atualizacao": (base - timedelta(days=60)).isoformat(),
                "operacao": "UPDATE",
            }
        )

    # Cancelamento: o histórico anterior precisa continuar valendo para as
    # transações antigas, mas o cartão não pode entrar em métrica futura.
    for cartao in rng.sample(registros[: len(ids_contas)], k=max(1, len(ids_contas) // 8)):
        registros.append(
            {
                **cartao,
                "status_cartao": "CANCELADO",
                "data_atualizacao": (base - timedelta(days=35)).isoformat(),
                "operacao": "UPDATE",
            }
        )

    # Cartão sem conta válida: órfão, vai para quarentena.
    registros.append(
        {
            "id_cartao": 99001,
            "id_conta": 777777,
            "tipo_cartao": "CREDITO",
            "limite": 5000.0,
            "status_cartao": "ATIVO",
            "data_atualizacao": (base - timedelta(days=30)).isoformat(),
            "operacao": "INSERT",
        }
    )

    return registros


def gerar_transacoes(cartoes: list[dict], base: date, dias: int) -> dict[str, list[dict]]:
    """Transações particionadas por dia.

    Retorna um dicionario nome_do_arquivo -> linhas. O particionamento por
    arquivo permite simular chegada fora de ordem e reprocesso.
    """
    rng = random.Random(SEMENTE + 3)
    ids_cartoes = sorted({c["id_cartao"] for c in cartoes if c["id_cartao"] < 90000})
    por_dia: dict[str, list[dict]] = {}
    id_transacao = 1

    for d in range(dias):
        dia = base - timedelta(days=dias - 1 - d)
        linhas: list[dict] = []

        for _ in range(rng.randint(60, 120)):
            mcc = rng.choice(MCCS)
            hora = rng.randint(0, 23)
            minuto = rng.randint(0, 59)
            linhas.append(
                {
                    "id_transacao": id_transacao,
                    "id_cartao": rng.choice(ids_cartoes),
                    "data_transacao": datetime(
                        dia.year, dia.month, dia.day, hora, minuto
                    ).isoformat(sep=" "),
                    "valor": round(rng.uniform(8, 3500), 2),
                    "mcc": mcc,
                    "estabelecimento": rng.choice(ESTABELECIMENTOS[mcc]),
                    "canal": rng.choice(CANAIS),
                    "pais": "BR" if rng.random() > 0.05 else "US",
                    "moeda": "BRL",
                }
            )
            id_transacao += 1

        por_dia[f"transacoes_{dia.isoformat()}.csv"] = linhas

    dias_ordenados = sorted(por_dia)

    # Valor zero e valor negativo: vao para quarentena.
    primeiro = por_dia[dias_ordenados[0]]
    primeiro.append({**primeiro[0], "id_transacao": 900001, "valor": 0.0})
    primeiro.append({**primeiro[1], "id_transacao": 900002, "valor": -250.0})

    # Transação apontando para cartão inexistente: órfã.
    primeiro.append({**primeiro[2], "id_transacao": 900003, "id_cartao": 555555})

    # Mesma transação repetida no arquivo do dia seguinte: reprocesso parcial.
    # Depois do MERGE a linha precisa existir uma única vez.
    segundo = por_dia[dias_ordenados[1]]
    segundo.extend(primeiro[:5])

    # Último arquivo tem coluna nova: evolução de schema. A camada Bronze
    # precisa preservar o campo sem quebrar a carga.
    ultimo = por_dia[dias_ordenados[-1]]
    for linha in ultimo:
        linha["dispositivo"] = rng.choice(["CHIP", "APROXIMACAO", "TARJA", "ONLINE"])

    return por_dia


def gerar_estornos(por_dia: dict[str, list[dict]], base: date) -> list[dict]:
    """Estornos totais, parciais e um apontando para transação inexistente."""
    rng = random.Random(SEMENTE + 4)
    todas = [linha for linhas in por_dia.values() for linha in linhas if linha["valor"] > 0]
    registros: list[dict] = []
    id_estorno = 1

    for transacao in rng.sample(todas, k=max(2, len(todas) // 25)):
        total = rng.random() > 0.4
        registros.append(
            {
                "id_estorno": id_estorno,
                "id_transacao": transacao["id_transacao"],
                "data_estorno": (base - timedelta(days=rng.randint(0, 5))).isoformat(),
                "valor_estorno": round(
                    transacao["valor"] if total else transacao["valor"] * 0.5, 2
                ),
                "motivo": rng.choice(
                    ["NAO_RECONHECIDO", "DUPLICIDADE", "CANCELAMENTO_COMPRA", "FRAUDE"]
                ),
            }
        )
        id_estorno += 1

    # Estorno de transação que não existe: referência inválida.
    registros.append(
        {
            "id_estorno": id_estorno,
            "id_transacao": 4242424,
            "data_estorno": base.isoformat(),
            "valor_estorno": 199.90,
            "motivo": "NAO_RECONHECIDO",
        }
    )

    return registros


def gerar_eventos_risco(por_dia: dict[str, list[dict]], base: date) -> list[dict]:
    """Eventos de fraude, chargeback e suspeita, com um vínculo inválido."""
    rng = random.Random(SEMENTE + 5)
    todas = [linha for linhas in por_dia.values() for linha in linhas]
    registros: list[dict] = []
    id_evento = 1

    for transacao in rng.sample(todas, k=max(2, len(todas) // 30)):
        tipo = rng.choice(["FRAUDE_CONFIRMADA", "CHARGEBACK", "SUSPEITA", "ALERTA_VELOCIDADE"])
        registros.append(
            {
                "id_evento": id_evento,
                "id_transacao": transacao["id_transacao"],
                "tipo_evento": tipo,
                "severidade": (
                    "ALTA" if tipo in ("FRAUDE_CONFIRMADA", "CHARGEBACK") else "MEDIA"
                ),
                "data_evento": (base - timedelta(days=rng.randint(0, 5))).isoformat(),
            }
        )
        id_evento += 1

    # Evento apontando para transação inexistente: vínculo inválido.
    registros.append(
        {
            "id_evento": id_evento,
            "id_transacao": 7777777,
            "tipo_evento": "SUSPEITA",
            "severidade": "BAIXA",
            "data_evento": base.isoformat(),
        }
    )

    return registros


def gerar_massa(settings: Settings, qtd_clientes: int = 200, dias: int = 5) -> dict[str, int]:
    """Escreve todos os arquivos de origem na landing.

    Returns:
        Contagem de linhas por origem, usada nos testes e no log da execução.
    """
    base = settings.data_referencia
    raiz = Path(settings.raiz_landing)

    clientes = gerar_clientes(qtd_clientes, base)
    contas = gerar_contas(clientes, base)
    cartoes = gerar_cartoes(contas, base)
    por_dia = gerar_transacoes(cartoes, base, dias)
    estornos = gerar_estornos(por_dia, base)
    eventos = gerar_eventos_risco(por_dia, base)

    _escrever_jsonl(raiz / "clientes" / "clientes_cdc.json", clientes)

    _escrever_csv(
        raiz / "contas" / "contas_cdc.csv",
        contas,
        ["id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura",
         "data_atualizacao", "operacao"],
    )
    _escrever_csv(
        raiz / "cartoes" / "cartoes_cdc.csv",
        cartoes,
        ["id_cartao", "id_conta", "tipo_cartao", "limite", "status_cartao",
         "data_atualizacao", "operacao"],
    )

    colunas_base = ["id_transacao", "id_cartao", "data_transacao", "valor", "mcc",
                    "estabelecimento", "canal", "pais", "moeda"]
    for nome, linhas in sorted(por_dia.items()):
        colunas = colunas_base + (["dispositivo"] if "dispositivo" in linhas[0] else [])
        _escrever_csv(raiz / "transacoes" / nome, linhas, colunas)

    _escrever_csv(
        raiz / "estornos" / "estornos.csv",
        estornos,
        ["id_estorno", "id_transacao", "data_estorno", "valor_estorno", "motivo"],
    )
    _escrever_csv(
        raiz / "eventos_risco" / "eventos_risco.csv",
        eventos,
        ["id_evento", "id_transacao", "tipo_evento", "severidade", "data_evento"],
    )

    resumo = {
        "clientes": len(clientes),
        "contas": len(contas),
        "cartoes": len(cartoes),
        "transacoes": sum(len(v) for v in por_dia.values()),
        "estornos": len(estornos),
        "eventos_risco": len(eventos),
    }
    logger.info("massa gerada", extra={"resumo": resumo, "landing": str(raiz)})
    return resumo
