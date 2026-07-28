# Contratos de dados

O que cada camada garante a quem consome.

## Bronze

**Garante:** cópia fiel do arquivo de origem, sem transformação. Nenhuma linha é
descartada.

**Não garante:** tipo, unicidade, integridade referencial ou domínio. Toda coluna
de negócio é string.

**Consumidor:** engenharia de dados, para reprocessamento e auditoria. Não é
camada de consumo analítico.

**Idempotência:** reprocessar o mesmo arquivo não insere linha nova. O controle é
por `arquivo_origem`.

## Prata: dimensões

### `dim_cliente`

Grão: uma linha por versão de cliente. Chave de negócio `id_cliente`, chave física
`id_cliente` mais `dw_inicio_vigencia`.

| Coluna | Tipo | Garantia |
|---|---|---|
| `id_cliente` | long | Não nulo |
| `cpf` | string | Não nulo, exatamente 11 dígitos |
| `estado` | string | UF brasileira válida, maiúscula |
| `renda` | decimal(18,2) | Nulo ou maior ou igual a zero |
| `data_atualizacao` | timestamp | Não nulo |
| `dw_inicio_vigencia` | timestamp | Igual à `data_atualizacao` da origem |
| `dw_fim_vigencia` | timestamp | Nulo na versão corrente |
| `dw_versao_ativa` | boolean | No máximo uma true por `id_cliente` |

Invariantes:

1. No máximo uma versão ativa por chave.
2. `dw_fim_vigencia > dw_inicio_vigencia` quando não nulo.
3. O fim de uma versão é exatamente o início da seguinte.
4. Exatamente uma versão responde por qualquer instante dentro do período de
   existência da entidade.

As três primeiras são verificáveis pelas consultas em
[`sql/02_scd_e_carga_incremental.sql`](../sql/02_scd_e_carga_incremental.sql).
Qualquer linha retornada ali é defeito.

Rejeições: `cliente_id_obrigatorio`, `cliente_cpf_obrigatorio`,
`cliente_cpf_11_digitos`, `cliente_renda_nao_negativa`, `cliente_uf_valida`,
`cliente_data_atualizacao_obrigatoria`.

### `dim_conta`

Mesma estrutura. Chave `id_conta`, atributos versionados `id_cliente`,
`tipo_conta`, `status_conta`, `data_abertura`.

`id_cliente` existe em `dim_cliente`. Conta órfã vai para quarentena com motivo
`conta_sem_cliente_valido`. `status_conta` em ATIVA, ENCERRADA, SUSPENSA,
BLOQUEADA.

### `dim_cartao`

Chave `id_cartao`, atributos versionados `id_conta`, `tipo_cartao`, `limite`,
`status_cartao`.

`id_conta` existe em `dim_conta`. Cartão órfão vai para quarentena com motivo
`cartao_sem_conta_valida`. `status_cartao` em ATIVO, CANCELADO, BLOQUEADO.

## Prata: fatos

### `fato_transacao`

Grão: uma linha por transação, `id_transacao` único.

| Coluna | Garantia |
|---|---|
| `id_transacao` | Não nulo, único na tabela |
| `id_cartao` | Não nulo, existe em `dim_cartao` |
| `data_transacao` | Não nulo, não futura |
| `valor` | Maior que zero |
| `moeda` | BRL, USD ou EUR |
| `data_particao` | Derivada de `data_transacao`, coluna de partição |

Deduplicação: a mesma transação em arquivos diferentes resulta em uma linha,
vencendo a de `timestamp_ingestao` mais recente.

### `fato_estorno`

Grão: uma linha por estorno. Uma transação pode ter vários.

`id_transacao` existe em `fato_transacao` e `valor_estorno` é maior que zero.

Não garante que `valor_estorno` seja menor ou igual ao valor da transação. A
origem pode enviar estorno maior, e a Ouro expõe o líquido resultante sem truncar,
para que a inconsistência fique visível.

### `fato_evento_risco`

Grão: uma linha por evento. `id_transacao` existe em `fato_transacao` e
`severidade` está em BAIXA, MEDIA, ALTA, CRITICA.

## Ouro

### `gold_fato_transacao`

Grão: uma linha por transação. Contagem idêntica a `fato_transacao`.

| Coluna | Garantia |
|---|---|
| `valor_bruto` | Valor original da transação |
| `valor_estornado` | Soma dos estornos, zero quando não há |
| `valor_liquido` | `valor_bruto - valor_estornado` |
| `foi_estornada` | True quando `qtd_estornos > 0` |
| `estorno_total` | True quando `valor_estornado >= valor_bruto` |
| `*_na_data` | Atributo da dimensão vigente em `data_transacao` |
| `elegivel_metrica` | Cartão e conta ativos na data da transação |

Invariante: `SUM(valor_liquido) = SUM(valor_bruto) - SUM(valor_estornado)`.

Métrica de comportamento comercial deve filtrar `elegivel_metrica = true`.
Métrica de risco não deve, porque fraude em cartão cancelado continua sendo
fraude.

### `gold_cliente_mes`

Grão: uma linha por cliente e mês. Considera apenas transações elegíveis. Valores
usam o líquido; `qtd_transacoes_estornadas` conta ocorrências.

`comprometimento_renda` é nulo quando a renda é nula ou zero, e não zero: não
saber é diferente de ser zero.

### `gold_indicadores_risco`

Grão: uma linha por dia. Considera todas as transações, inclusive as não
elegíveis.

### `gold_features_cliente`

Grão: uma linha por cliente. Todo cliente da dimensão aparece, inclusive quem não
transacionou, com contagens em zero.

Todas as features usam `f_data_referencia`, gravada na própria tabela. Reproduzir
a base de uma data passada é reexecutar com `--data-referencia` daquele dia.

Prefixo `f_` marca coluna consumível como feature.

### `gold_dim_cliente`, `gold_dim_conta`, `gold_dim_cartao`

Grão: uma linha por chave, versão corrente da dimensão da Prata.

Nenhuma coluna `dw_*`. `vigente_desde` informa desde quando o cadastro atual vale.

Não garante histórico. Para consulta pontual por data use a Prata; para o cadastro
no momento da transação use as colunas `*_na_data` do fato.

### `gold_dim_estabelecimento`

Grão: uma linha por estabelecimento e MCC. Derivada do movimento, já que não
existe cadastro de estabelecimento na origem.

## Quarentena

Toda linha rejeitada em qualquer camada aparece em
`quarentena.registros_rejeitados`, com `entidade`, `camada_origem`, `batch_id`,
`data_referencia`, `quarentenado_em`, `motivos_rejeicao`, `registro_original` e
`hash_rejeicao`.

Idempotência: gravação por MERGE em `hash_rejeicao`. Reprocessar não duplica. Se o
conteúdo mudar na origem ou passar a violar outra regra, o hash muda e entra como
rejeição nova. O `batch_id` gravado é o da primeira detecção.

Não há reprocessamento automático a partir da quarentena: reinjetar sem saber o
que mudou reintroduziria o mesmo defeito.

## Observabilidade

`observabilidade.execucao_pipeline` recebe uma linha por etapa executada,
inclusive as que falharam, com `batch_id`, `data_referencia`, `camada`, `etapa`,
`entidade`, `linhas_lidas`, `linhas_gravadas`, `linhas_rejeitadas`,
`duracao_segundos`, `status` e `detalhe_erro`.

## SLA proposto

Não implementado, é o desenho para produção.

| Item | Alvo |
|---|---|
| Disponibilidade da Ouro | Até 07h para o movimento de D-1 |
| Frescor máximo | 1 dia |
| Taxa de rejeição | Abaixo de 1% por entidade |
| Retenção da Bronze | 5 anos |
| Retenção da quarentena | 1 ano |
| Time travel do Delta | 30 dias |

As consultas que medem esses itens estão em
[`sql/03_observabilidade.sql`](../sql/03_observabilidade.sql).
