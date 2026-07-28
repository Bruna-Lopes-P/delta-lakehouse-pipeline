"""Log estruturado em JSON.

O driver do Databricks captura stdout. Emitir JSON em vez de texto livre permite
consultar a execução depois (por batch_id, por etapa, por severidade) sem parsear
mensagem humana.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

_CAMPOS_PADRAO = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName", "message", "asctime",
}


class FormatadorJson(logging.Formatter):
    """Serializa o LogRecord em uma linha JSON.

    Qualquer keyword passada via ``extra=`` entra no JSON, o que permite anexar
    batch_id, etapa e contadores ao evento sem concatenar string.
    """

    def format(self, record: logging.LogRecord) -> str:
        evento: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "nivel": record.levelname,
            "origem": record.name,
            "mensagem": record.getMessage(),
        }

        for chave, valor in record.__dict__.items():
            if chave not in _CAMPOS_PADRAO and not chave.startswith("_"):
                evento[chave] = valor

        if record.exc_info:
            evento["excecao"] = self.formatException(record.exc_info)

        return json.dumps(evento, ensure_ascii=False, default=str)


def configurar_logs(nivel: str = "INFO") -> None:
    """Instala o formatador JSON na raiz. Idempotente."""
    raiz = logging.getLogger()
    raiz.setLevel(nivel)

    for handler in list(raiz.handlers):
        raiz.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(FormatadorJson())
    raiz.addHandler(handler)

    # py4j e verborragico e polui o log da aplicação.
    logging.getLogger("py4j").setLevel(logging.WARNING)


def obter_logger(nome: str) -> logging.Logger:
    """Logger da aplicação, sob o namespace novarota."""
    return logging.getLogger(f"novarota.{nome}")
