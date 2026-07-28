# Invariantes verificadas

Checagens sobre o resultado das duas execuções. Qualquer FALHOU aqui é
defeito de carga, não característica do dado.

| verificação                                              | obtido | esperado | resultado |
|----------------------------------------------------------|--------|----------|-----------|
| dim_cliente: chaves com mais de uma versão ativa         | 0      | 0        | ok        |
| dim_cliente: intervalos de vigência invertidos ou vazios | 0      | 0        | ok        |
| dim_cliente: versões históricas preservadas              | 28     | > 0      | ok        |
| dim_conta: chaves com mais de uma versão ativa           | 0      | 0        | ok        |
| dim_conta: intervalos de vigência invertidos ou vazios   | 0      | 0        | ok        |
| dim_conta: versões históricas preservadas                | 38     | > 0      | ok        |
| dim_cartao: chaves com mais de uma versão ativa          | 0      | 0        | ok        |
| dim_cartao: intervalos de vigência invertidos ou vazios  | 0      | 0        | ok        |
| dim_cartao: versões históricas preservadas               | 163    | > 0      | ok        |
| fato_transacao: transações duplicadas                    | 0      | 0        | ok        |
| gold: líquido == bruto - estornado                       | True   | True     | ok        |
| gold: grão de uma linha por transação                    | True   | True     | ok        |
| gold concilia com a Prata em contagem                    | True   | True     | ok        |
| gold: transações de cartão cancelado preservadas         | 79     | > 0      | ok        |

Falhas: 0
