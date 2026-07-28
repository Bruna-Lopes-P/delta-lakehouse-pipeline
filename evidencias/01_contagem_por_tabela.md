# Contagem por tabela

Duas execuções completas sobre a mesma massa. A coluna de comparacao e a
prova de idempotência: reprocessar não perde nem duplica.

A única tabela que cresce é a de observabilidade, e ela deve crescer: registra
que o pipeline executou, e a segunda execução é um evento novo. Idempotência é
propriedade do dado processado, não do log de quem processou.

| tabela                            | 1a execução | 2a execução | comparação        |
|-----------------------------------|-------------|-------------|-------------------|
| bronze.clientes                   | 233         | 233         | igual             |
| bronze.contas                     | 419         | 419         | igual             |
| bronze.cartoes                    | 747         | 747         | igual             |
| bronze.transacoes                 | 390         | 390         | igual             |
| bronze.estornos                   | 16          | 16          | igual             |
| bronze.eventos_risco              | 14          | 14          | igual             |
| silver.dim_cliente                | 228         | 228         | igual             |
| silver.dim_conta                  | 418         | 418         | igual             |
| silver.dim_cartao                 | 746         | 746         | igual             |
| silver.fato_transacao             | 382         | 382         | igual             |
| silver.fato_estorno               | 14          | 14          | igual             |
| silver.fato_evento_risco          | 13          | 13          | igual             |
| gold.gold_fato_transacao          | 382         | 382         | igual             |
| gold.gold_cliente_mes             | 136         | 136         | igual             |
| gold.gold_indicadores_risco       | 5           | 5           | igual             |
| gold.gold_features_cliente        | 200         | 200         | igual             |
| gold.gold_dim_estabelecimento     | 16          | 16          | igual             |
| gold.gold_dim_cliente             | 200         | 200         | igual             |
| gold.gold_dim_conta               | 380         | 380         | igual             |
| gold.gold_dim_cartao              | 583         | 583         | igual             |
| quarentena.registros_rejeitados   | 12          | 12          | igual             |
| observabilidade.execucao_pipeline | 13          | 26          | cresce por design |

Divergencias inesperadas: 0
