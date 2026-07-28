# Arquitetura

Data product transacional em arquitetura Lakehouse sobre Databricks e Delta Lake.

## Visão geral

```
ORIGENS (landing)
   clientes_cdc.json   contas_cdc.csv    cartoes_cdc.csv
   transacoes/*.csv    estornos.csv      eventos_risco.csv
        |
        |  ingestão incremental, controle de arquivo processado
        v
BRONZE
   cópia fiel da origem, tudo como string
   + arquivo_origem, data_ingestao, timestamp_ingestao,
     batch_id, hash_linha, schema_version
        |
        |  tipagem, qualidade, integridade referencial
        v
PRATA
   dimensões SCD Tipo 2        fatos com MERGE por chave
     dim_cliente                 fato_transacao
     dim_conta                   fato_estorno
     dim_cartao                  fato_evento_risco
        |                              |
        | rejeitados                   | join temporal
        v                              v
QUARENTENA                        OURO
   registros_rejeitados             gold_fato_transacao
   motivo + registro original        gold_cliente_mes
                                     gold_indicadores_risco
                                     gold_features_cliente
                                     gold_dim_cliente/conta/cartao
                                     gold_dim_estabelecimento

OBSERVABILIDADE
   execucao_pipeline: lidas, gravadas, rejeitadas, duração, status
```

## Camada Bronze

Guarda o que chegou, do jeito que chegou. Não limpa, não tipa e não descarta.

Lê tudo como string para evitar que o `inferSchema` decida tipos diferentes em
cargas diferentes. Metadados anexados a cada linha:

| Coluna | Serve para |
|---|---|
| `arquivo_origem` | Rastrear a procedência de cada linha |
| `data_ingestao` | Data lógica da carga |
| `timestamp_ingestao` | Instante real, usado no desempate da deduplicação |
| `batch_id` | Agrupar tudo que entrou na mesma execução |
| `hash_linha` | Detectar conteúdo idêntico entre cargas |
| `schema_version` | Identificar quando o layout da origem mudou |

Idempotência: a tabela guarda `arquivo_origem` e a ingestão pula o que já entrou.
Arquivos com colunas diferentes são unidos com
`unionByName(allowMissingColumns=True)`.

## Camada Prata

Sequência por entidade: tipagem explícita, regras de qualidade, integridade
referencial e persistência.

### Dimensões com SCD Tipo 2

| Coluna | Significado |
|---|---|
| `dw_hash_atributos` | Hash dos atributos versionados |
| `dw_inicio_vigencia` | Início da validade, vindo da origem |
| `dw_fim_vigencia` | Fim da validade, nulo na versão corrente |
| `dw_versao_ativa` | Marca a versão corrente |
| `dw_excluido` | Exclusão lógica vinda de um DELETE do CDC |
| `dw_batch_id` | Execução que gerou a versão |
| `dw_atualizado_em` | Instante do processamento |

Vigência em intervalo `[início, fim)`. Consulta pontual:

```sql
WHERE dw_inicio_vigencia <= :momento
  AND (dw_fim_vigencia IS NULL OR dw_fim_vigencia > :momento)
```

O MERGE emite duas linhas por alteração: uma casa com a versão vigente e a fecha,
outra tem a chave de merge nula, não casa e por isso é inserida. Um MERGE sozinho
não faz as duas coisas, porque cada linha da origem dispara no máximo uma ação.

Atributos versionados:

| Entidade | Chave | Atributos |
|---|---|---|
| `dim_cliente` | `id_cliente` | nome, cidade, estado, renda, segmento |
| `dim_conta` | `id_conta` | id_cliente, tipo_conta, status_conta, data_abertura |
| `dim_cartao` | `id_cartao` | id_conta, tipo_cartao, limite, status_cartao |

### Fatos com MERGE por chave

Fato não versiona. A deduplicação mantém a linha de ingestão mais recente, o que
resolve o reenvio da mesma transação em arquivos diferentes.

### Ordem de processamento

`dim_cliente`, `dim_conta`, `dim_cartao`, `fato_transacao`, depois `fato_estorno`
e `fato_evento_risco`. A validação de integridade de cada etapa depende da
anterior já estar materializada.

## Camada Ouro

| Produto | Grão | Uso |
|---|---|---|
| `gold_fato_transacao` | transação | Base de tudo, cadastro resolvido na data |
| `gold_cliente_mes` | cliente e mês | Comportamento mensal |
| `gold_indicadores_risco` | dia | Fraude, chargeback, estorno |
| `gold_features_cliente` | cliente | Consumo por ciência de dados |
| `gold_dim_cliente/conta/cartao` | chave | Cadastro corrente, sem colunas do SCD |
| `gold_dim_estabelecimento` | estabelecimento e MCC | Curva ABC, taxa de estorno |

Regras de negócio:

* **Join temporal.** Cada transação enxerga o cadastro vigente na data em que
  ocorreu.
* **Estorno.** `valor_liquido = valor_bruto - valor_estornado`. Estorno parcial
  abate só a parte estornada; estornos múltiplos são agregados antes do join.
* **Cartão cancelado.** `elegivel_metrica` avalia o status vigente na data. A
  compra feita com o cartão ativo continua somando no mês em que ocorreu.
* **Risco.** `gold_indicadores_risco` não aplica `elegivel_metrica`, porque
  fraude em cartão cancelado continua sendo fraude.

## Quarentena

Registro rejeitado vai para `quarentena.registros_rejeitados` com `entidade`,
`camada_origem`, `motivos_rejeicao`, `batch_id`, `data_referencia`,
`registro_original` em JSON e `hash_rejeicao`.

A gravação usa MERGE por `hash_rejeicao`, não append: a Prata reprocessa a Bronze
inteira, então os mesmos inválidos reprovam de novo a cada execução.

Motivos cobertos: formato e domínio (CPF, UF, valor, status, moeda) e vínculo
(`cartao_sem_conta_valida`, `transacao_sem_cartao_valido`,
`estorno_sem_transacao_valida`, `evento_sem_transacao_valida`).

## Observabilidade

Cada etapa registra em `observabilidade.execucao_pipeline`: `batch_id`,
`data_referencia`, `camada`, `etapa`, `entidade`, `linhas_lidas`,
`linhas_gravadas`, `linhas_rejeitadas`, `duracao_segundos`, `status` e
`detalhe_erro`.

Em tabela e não apenas no log porque a pergunta operacional é sobre série
histórica: o volume caiu em relação à média da semana, a taxa de rejeição subiu
depois do deploy. O log da aplicação sai em JSON por evento, o que permite
filtrar por `batch_id` sem parsear texto.

Consultas de monitoramento em [`sql/03_observabilidade.sql`](../sql/03_observabilidade.sql).

## Estratégia de joins

A Ouro tem quatro joins encadeados sobre a maior tabela do modelo.

| Join | Tipo | Estratégia esperada |
|---|---|---|
| fato com estornos agregados | left | broadcast |
| fato com `dim_cartao` | left, com faixa de vigência | broadcast |
| fato com `dim_conta` | left, com faixa de vigência | broadcast |
| fato com `dim_cliente` | left, com faixa de vigência | broadcast |
| filho com pai na integridade | left semi ou left anti | broadcast |

O join temporal tem uma igualdade e duas comparações de faixa. O otimizador usa a
igualdade para distribuir, e a faixa vira filtro pós-join. Enquanto a dimensão
couber em broadcast, cada executor tem a dimensão inteira e resolve a faixa
localmente, sem shuffle.

Quando não couber, o plano degrada para sort-merge join. Os caminhos, na ordem em
que valeria tentar:

1. Reduzir a dimensão antes do join, filtrando versões que não intersectam a
   janela sendo processada.
2. Pré-materializar a versão vigente por dia, trocando o join por faixa por um
   join de igualdade em (chave, dia).
3. Aumentar `autoBroadcastJoinThreshold`, que é o ajuste mais barato mas só adia
   o problema.

O notebook tem uma célula com `EXPLAIN FORMATTED` sobre o fato. O que procurar:
`BroadcastHashJoin` nos joins com dimensão, `PushedFilters` na leitura do Delta e
o número de partições lidas.

## Arquivos pequenos

Cada MERGE gera arquivos novos, e a Prata faz um MERGE por entidade por execução.
Sem compactação, o número de arquivos cresce com o número de execuções e a leitura
degrada mesmo com volume estável.

O pipeline chama `OPTIMIZE` ao fim das camadas Prata e Ouro, com `ZORDER` em
`id_cartao` e `id_transacao` no fato e `id_cliente` na dimensão. Onde houver
Liquid Clustering, ele substitui partição mais Z-ORDER.

## Governança no Unity Catalog

Com `NOVAROTA_USAR_UC=true`, as tabelas passam a ser gerenciadas em
`catalogo.schema.tabela`.

```
novarota
  bronze            leitura restrita à engenharia
  silver            leitura para analytics e ciência de dados
  gold              leitura ampla, é a camada de consumo
  quarentena        leitura para engenharia e qualidade
  observabilidade   leitura para engenharia e operação
```

Fora de produção os schemas ganham sufixo de ambiente (`bronze_dev`), o que
permite compartilhar o catálogo sem colisão.

CPF é dado pessoal e a Bronze o guarda em claro. Em produção o acesso à Bronze
fica restrito e a Prata expõe o CPF mascarado por column mask.

## Agendamento

Em Databricks Workflows, uma task por camada com dependência linear:

```
gerar_massa (só em dev)  ->  bronze  ->  silver  ->  gold
```

Parâmetros por execução: `--ambiente`, `--modo`, `--data-referencia`,
`--batch-id`. O `--modo` permite reexecutar uma camada isolada, que é o caminho
normal quando a Ouro precisa ser recalculada após correção de regra.

Job cluster efêmero em vez de all-purpose. Retry na task e não no job inteiro,
já que cada etapa é idempotente.

## Estrutura do repositório

```
src/
  config/          parametrização da execução
  utils/           sessão Spark, gravação Delta, log JSON
  datagen/         geração da massa sintética
  ingestion/       camada Bronze
  quality/         regras, quarentena, integridade
  silver/          SCD Tipo 2, dimensões, fatos
  gold/            data products
  observability/   métricas de execução
  pipeline/        orquestração

tests/       suíte automatizada
sql/         consultas analíticas e de monitoramento
notebooks/   execução no Databricks
docs/        arquitetura, decisões, contratos
```

Camada não importa camada: `silver` não conhece `gold`, e o acoplamento fica em
`pipeline/run.py`. Notebook é casca de execução e não contém regra.
