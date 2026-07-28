# Métricas por etapa

Uma linha por etapa executada, gravada em Delta pelo proprio pipeline.

| batch       | camada | etapa          | entidade          | lidas | gravadas | rejeitadas | segundos | status  |
|-------------|--------|----------------|-------------------|-------|----------|------------|----------|---------|
| evidencia01 | bronze | ingestao       | clientes          | 233   | 233      | 0          | 165      | SUCESSO |
| evidencia01 | bronze | ingestao       | contas            | 419   | 419      | 0          | 152      | SUCESSO |
| evidencia01 | bronze | ingestao       | cartoes           | 747   | 747      | 0          | 150      | SUCESSO |
| evidencia01 | bronze | ingestao       | transacoes        | 390   | 390      | 0          | 148      | SUCESSO |
| evidencia01 | bronze | ingestao       | estornos          | 16    | 16       | 0          | 146      | SUCESSO |
| evidencia01 | bronze | ingestao       | eventos_risco     | 14    | 14       | 0          | 144      | SUCESSO |
| evidencia01 | silver | transformacao  | dim_cliente       | 233   | 228      | 4          | 143      | SUCESSO |
| evidencia01 | silver | transformacao  | dim_conta         | 419   | 418      | 1          | 137      | SUCESSO |
| evidencia01 | silver | transformacao  | dim_cartao        | 747   | 746      | 1          | 125      | SUCESSO |
| evidencia01 | silver | transformacao  | fato_transacao    | 390   | 382      | 3          | 105      | SUCESSO |
| evidencia01 | silver | transformacao  | fato_estorno      | 16    | 14       | 2          | 78       | SUCESSO |
| evidencia01 | silver | transformacao  | fato_evento_risco | 14    | 13       | 1          | 59       | SUCESSO |
| evidencia01 | gold   | materializacao | todos             | 0     | 1902     | 0          | 31       | SUCESSO |
| evidencia02 | bronze | ingestao       | clientes          | 0     | 0        | 0          | 126      | SUCESSO |
| evidencia02 | bronze | ingestao       | contas            | 0     | 0        | 0          | 126      | SUCESSO |
| evidencia02 | bronze | ingestao       | cartoes           | 0     | 0        | 0          | 126      | SUCESSO |
| evidencia02 | bronze | ingestao       | transacoes        | 0     | 0        | 0          | 125      | SUCESSO |
| evidencia02 | bronze | ingestao       | estornos          | 0     | 0        | 0          | 124      | SUCESSO |
| evidencia02 | bronze | ingestao       | eventos_risco     | 0     | 0        | 0          | 123      | SUCESSO |
| evidencia02 | silver | transformacao  | dim_cliente       | 233   | 0        | 4          | 123      | SUCESSO |
| evidencia02 | silver | transformacao  | dim_conta         | 419   | 0        | 1          | 112      | SUCESSO |
| evidencia02 | silver | transformacao  | dim_cartao        | 747   | 0        | 1          | 101      | SUCESSO |
| evidencia02 | silver | transformacao  | fato_transacao    | 390   | 382      | 3          | 97       | SUCESSO |
| evidencia02 | silver | transformacao  | fato_estorno      | 16    | 14       | 2          | 73       | SUCESSO |
| evidencia02 | silver | transformacao  | fato_evento_risco | 14    | 13       | 1          | 68       | SUCESSO |
| evidencia02 | gold   | materializacao | todos             | 0     | 1902     | 0          | 57       | SUCESSO |
