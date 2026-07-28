"""Conversão de tipo tolerante a dado malformado.

O cast direto (``col.cast("timestamp")``) lança exceção quando o valor não
converte e o modo ANSI está ligado, que é o padrão do Spark 4 e do Databricks
Runtime recente. Com ANSI desligado o mesmo cast devolve nulo em silêncio, então
o comportamento do pipeline mudaria conforme a versão do engine.

A Prata precisa da segunda semântica de forma explícita: valor que não converte
vira nulo, a regra de qualidade reprova a linha e ela vai para a quarentena com
o motivo. Sem isso, um único CPF com data torta derruba a carga inteira em vez
de isolar o registro.

``try_cast`` existe como função SQL desde o Spark 3.2 e se comporta igual nas
duas configurações de ANSI.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def converter(coluna: str, tipo: str) -> Column:
    """Converte a coluna, devolvendo nulo quando o valor não é conversível.

    Args:
        coluna: nome da coluna de origem.
        tipo: tipo SQL de destino, como ``bigint`` ou ``decimal(18,2)``.
    """
    return F.expr(f"try_cast(`{coluna}` AS {tipo})")


def texto(coluna: str) -> Column:
    """Normaliza texto livre: remove espaço nas pontas."""
    return F.trim(F.col(coluna))


def texto_padronizado(coluna: str) -> Column:
    """Normaliza texto de domínio: espaço nas pontas e caixa alta.

    Categoria que chega como "ativo", "Ativo" e " ATIVO " é a mesma categoria.
    Sem normalizar, a regra de domínio reprovaria as duas primeiras.
    """
    return F.upper(F.trim(F.col(coluna)))
