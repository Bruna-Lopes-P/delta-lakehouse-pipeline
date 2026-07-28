# Como estas evidencias foram geradas

```bash
python -m scripts.gerar_evidencias
```

O script gera a massa sintética, executa o pipeline duas vezes sobre ela e
grava os arquivos deste diretório. A segunda execução existe para evidenciar
idempotência: as contagens em `01_contagem_por_tabela.md` sao idênticas.

A massa usa semente fixa, entao rodar de novo produz os mesmos números.

## Parametros

| parâmetro            | valor       |
|----------------------|-------------|
| ambiente             | dev         |
| data de referência   | 2026-03-31  |
| batch da 1a execução | evidencia01 |
| batch da 2a execução | evidencia02 |
| clientes na massa    | 200         |
| dias de transação    | 5           |

## Arquivos

| arquivo | conteúdo |
|---|---|
| `01_contagem_por_tabela.md` | linhas por tabela nas duas execuções |
| `02_metricas_por_etapa.md` | lidas, gravadas, rejeitadas e duração por etapa |
| `03_quarentena.md` | rejeições por motivo e exemplo preservado |
| `04_invariantes.md` | checagens sobre o resultado |
| `05_resumo.md` | totais da massa, da execução e do negócio |
