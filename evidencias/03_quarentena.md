# Quarentena

Total de registros rejeitados: 12

Todos os defeitos abaixo foram injetados de propósito na massa sintética.

| entidade     | motivo                       | quantidade |
|--------------|------------------------------|------------|
| cliente      | cliente_cpf_11_digitos       | 2          |
| transacao    | transacao_valor_positivo     | 2          |
| estorno      | estorno_sem_transacao_valida | 2          |
| evento_risco | evento_sem_transacao_valida  | 1          |
| transacao    | transacao_sem_cartao_valido  | 1          |
| conta        | conta_sem_cliente_valido     | 1          |
| cartao       | cartao_sem_conta_valida      | 1          |
| cliente      | cliente_renda_nao_negativa   | 1          |
| cliente      | cliente_cpf_obrigatorio      | 1          |
| cliente      | cliente_uf_valida            | 1          |

## Exemplo de registro preservado

- **evento_risco** | motivos: `evento_sem_transacao_valida`
  ```json
  {"id_evento":14,"id_transacao":7777777,"tipo_evento":"SUSPEITA","severidade":"BAIXA","data_evento":"2026-03-31","arquivo_origem":"file:/C:/tmp/delta-lakehouse-pipeline/evidencias/landing/eventos_risco/eventos_risco.csv","batch_id":"evidencia01","timestamp_ingestao":"2026-07-27T18:15:24.137-03:00"}
  ```
- **conta** | motivos: `conta_sem_cliente_valido`
  ```json
  {"id_conta":99001,"id_cliente":888888,"tipo_conta":"CORRENTE","status_conta":"ATIVA","data_abertura":"2025-06-04","data_atualizacao":"2026-03-01T00:00:00.000-03:00","operacao":"INSERT","arquivo_origem":"file:/C:/tmp/delta-lakehouse-pipeline/evidencias/landing/contas/contas_cdc.csv","batch_id":"evide
  ```
- **cartao** | motivos: `cartao_sem_conta_valida`
  ```json
  {"id_cartao":99001,"id_conta":777777,"tipo_cartao":"CREDITO","limite":5000.00,"status_cartao":"ATIVO","data_atualizacao":"2026-03-01T00:00:00.000-03:00","operacao":"INSERT","arquivo_origem":"file:/C:/tmp/delta-lakehouse-pipeline/evidencias/landing/cartoes/cartoes_cdc.csv","batch_id":"evidencia01"}
  ```
