# Decisões técnicas

O que foi decidido, a alternativa descartada e o motivo.

## 1. Vigência do SCD vem da origem, não da data da carga

`dw_inicio_vigencia` recebe a `data_atualizacao` do registro na origem. A data de
processamento fica só em `dw_atualizado_em`, como metadado.

**Alternativa descartada:** carimbar `current_date()` na carga.

**Motivo:** a pergunta que o SCD Tipo 2 responde é qual era o cadastro no dia do
fato. Com a data da carga, uma alteração ocorrida no dia 10 e carregada no dia 13
faz o período de 10 a 12 responder com o cadastro antigo. O relatório fica errado
exatamente nos dias em que houve atraso, que é quando ninguém está olhando.

Como efeito, a carga fica insensível ao momento em que roda: reprocessar um dia
antigo produz a mesma linha do tempo.

`test_scd.py::TestPrimeiraCarga::test_vigencia_inicial_vem_da_origem_e_nao_da_data_da_carga`

## 2. Intervalo de vigência fechado no início e aberto no fim

Uma versão vale em `[dw_inicio_vigencia, dw_fim_vigencia)`. A corrente tem fim
nulo, e o fim de uma versão é exatamente o início da seguinte.

**Alternativa descartada:** fechar dos dois lados, com `fim = próximo_início - 1`.

**Motivo:** o intervalo fechado exige escolher a unidade a subtrair. Se for um
dia, duas alterações no mesmo dia produzem intervalo invertido. Se for um segundo,
o problema reaparece com duas alterações no mesmo segundo. A subtração ainda
precisa ser repetida em toda consulta que reconstrói a linha do tempo.

Com intervalo semiaberto a consulta é sempre a mesma e cobre a reta inteira sem
sobreposição:

```sql
WHERE dw_inicio_vigencia <= :momento
  AND (dw_fim_vigencia IS NULL OR dw_fim_vigencia > :momento)
```

`test_scd.py::TestNovaVersao::test_intervalos_nao_se_sobrepoem`

## 3. Idempotência da dimensão por hash de atributos

Antes de abrir versão nova, o hash dos atributos é comparado com o da versão
vigente. Iguais, nada acontece.

**Alternativa descartada:** confiar só no controle de arquivos já ingeridos.

**Motivo:** o controle de arquivo protege contra reprocessar o mesmo arquivo, mas
não contra a origem reenviar o mesmo conteúdo com outro nome, que é o caso comum
em reenvio de janela. Sem o hash, cada reenvio abriria uma versão idêntica.

O hash usa `<nulo>` como marcador para que `(A, nulo)` e `(nulo, A)` não colidam.

`test_scd.py::TestIdempotencia`

## 4. Registro atrasado é descartado

Registro com `data_atualizacao` anterior ou igual ao início da versão vigente, com
conteúdo diferente, é contado em `ignorados_atrasados` e não entra.

**Alternativa descartada:** inserir a versão no meio da linha do tempo,
reencadeando as vigências ao redor.

**Motivo:** reencadear exige reescrever versões já publicadas, o que invalida
relatórios fechados gerados a partir delas. Descartar é conservador: o dado atual
continua o mais recente conhecido e o descarte aparece na métrica da execução.

O caso de igualdade entra aqui porque aplicar fecharia a vigente com
`dw_fim_vigencia` igual ao próprio `dw_inicio_vigencia`, um intervalo vazio que
não responde por instante nenhum.

Correção retroativa exige reprocessar a entidade a partir da Bronze.

`test_scd.py::TestDadoAtrasado`

## 5. Só a primeira versão nova fecha a vigente

Quando a carga traz mais de uma versão nova da mesma chave, apenas a de menor
`dw_inicio_vigencia` entra no lado de fechamento do MERGE.

**Motivo:** com a dimensão já materializada, duas linhas de fechamento apontariam
para a mesma versão vigente e o Delta aborta com
`UnsupportedOperationException: Cannot perform Merge`. O caso não aparece na
primeira carga porque ali a tabela é criada por overwrite, sem MERGE.

`test_scd.py::TestVariasVersoesNaMesmaCarga`

## 6. Fato usa MERGE por chave natural, não SCD Tipo 2

**Motivo:** uma transação ocorreu uma vez, com um valor. Ela não muda depois; o
que existe são correções, e correção neste domínio chega como estorno, que é
outro fato com data própria. Versionar o fato criaria uma linha do tempo sem
correspondência no mundo real e dobraria o custo de leitura da maior tabela.

O MERGE ainda atualiza a linha quando a origem reenvia a mesma chave com conteúdo
diferente, o que cobre correção de erro de origem.

`test_pipeline.py::TestPipelineCompleto::test_nao_deixa_transacao_duplicada_na_prata`

## 7. Bronze lê tudo como string

A ingestão usa `inferSchema=false`. A conversão acontece na Prata.

**Motivo:** o `inferSchema` decide o tipo por amostragem, então o mesmo campo pode
virar tipos diferentes em cargas diferentes. Dois casos concretos: CPF com zero à
esquerda vira número e perde o zero; um campo numérico vazio vira string num dia e
double no outro, fazendo a união falhar por conflito de tipo.

Lendo como string, a Bronze preserva o byte que chegou e a conversão fica em um
lugar só, explícito e testado.

`test_pipeline.py::TestBronze::test_le_tudo_como_string`

## 8. Coluna nova da origem para na Bronze

A Bronze absorve qualquer coluna acrescentada. A Prata não: a tipagem faz `select`
explícito.

**Alternativa descartada:** propagar automaticamente até a Prata.

**Motivo:** propagar cria dois problemas. O MERGE dos fatos usa `updateAll` e
`insertAll`, que falham quando a origem traz coluna ausente no destino. E o schema
da Prata mudaria sem revisão, com quem consome descobrindo pela diferença.

Campo novo fica disponível na Bronze desde o dia em que chegou, e a
`schema_version` registra a partir de qual carga ele existe.

`test_pipeline.py::TestIdempotenciaDoPipeline::test_coluna_nova_na_bronze_nao_altera_o_schema_da_prata`

## 9. Auto Loader é o caminho de produção, com alternativa em batch

`ingerir_auto_loader` implementa a ingestão com `cloudFiles` e
`Trigger.AvailableNow`. `ingerir` implementa a leitura em batch com controle de
arquivos processados, e é o que o pipeline usa.

**Motivo:** em produção o Auto Loader é superior. Descobre arquivo novo por
notificação de evento do storage em vez de listar diretório, versiona o schema no
`schemaLocation` e traz `_rescued_data` para campo inesperado.

Ele depende de storage em nuvem e não roda na suíte local. Um pipeline cujo
caminho principal só pode ser exercitado com cluster ligado não tem teste de
verdade, então o caminho batch existe para que idempotência, qualidade e SCD sejam
testáveis em qualquer máquina. O contrato de idempotência é o mesmo nos dois.

`test_pipeline.py::TestBronze::test_nao_reingere_arquivo_ja_processado`

## 10. Registro rejeitado vai para quarentena

Toda linha reprovada vai para `quarentena.registros_rejeitados` com os motivos, o
batch e o registro original em JSON.

**Alternativa descartada:** filtrar com `.filter()` e seguir.

**Motivo:** filtro silencioso torna impossível responder o que foi perdido na
carga sem reprocessar a origem. Com a quarentena, a pergunta é uma consulta, o
volume rejeitado vira métrica e o registro pode ser reinjetado depois da correção.

A gravação usa MERGE por `hash_rejeicao`, não append: a Prata reprocessa a Bronze
inteira, então os mesmos inválidos reprovam de novo a cada execução. Com append, a
contagem por motivo deixaria de significar quantos registros estão com o problema.

`test_quality_rules.py::TestQuarentena`

## 11. Regras de qualidade são dado, não código imperativo

Cada regra é um objeto `Regra` com nome, descrição, condição e severidade.

**Alternativa descartada:** encadear `.filter()` nas transformações.

**Motivo:** com filtro encadeado a linha desaparece e não há como saber qual
condição a eliminou. Como dado, dá para apontar o motivo de cada rejeição, testar
a regra isolada e publicar o conjunto como contrato legível por quem não lê
PySpark.

A condição é envolvida em `coalesce(condicao, False)`. Sem isso, comparação com
coluna nula devolve NULL, que não é True nem False, e a linha escaparia do filtro
de rejeição sem aparecer em lugar nenhum.

`test_quality_rules.py::TestAplicarRegras::test_nulo_na_condicao_conta_como_violacao`

## 12. Integridade referencial trata órfão como caso de qualidade

Cartão sem conta, transação sem cartão e estorno sem transação vão para a mesma
quarentena, com motivo próprio.

**Motivo:** do ponto de vista de quem consome, uma transação que não se liga a um
cliente é tão inútil quanto uma com valor negativo. Tratar os dois no mesmo lugar
dá um só ponto de consulta para o que não entrou.

A separação usa `left anti join`: não é preciso trazer coluna nenhuma do pai,
apenas decidir presença. Um `left join` com filtro por nulo traria colunas
desnecessárias e multiplicaria linhas se a chave do pai tiver duplicata.

`test_quality_rules.py::TestIntegridadeReferencial`

## 13. Join com a dimensão é temporal

O fato da Ouro busca a versão vigente na `data_transacao`, não a corrente.

**Motivo:** com a versão corrente, toda mudança cadastral reescreve o passado. Um
cliente que era VAREJO em janeiro e virou PREMIUM em março apareceria como PREMIUM
na apuração de janeiro, e o relatório de um mês fechado mudaria sozinho entre duas
execuções. Isso quebra conciliação e destrói a confiança no número.

Como os intervalos de uma mesma chave não se sobrepõem, cada fato encontra no
máximo uma versão e o join não multiplica linhas.

`test_gold.py::TestJoinTemporal`

## 14. Cartão cancelado sai da métrica pelo status da data

`elegivel_metrica` avalia o status do cartão e da conta vigentes na data da
transação.

**Motivo:** o requisito é que o cancelado não componha métrica futura mas preserve
histórico. Filtrar pelo status atual apagaria retroativamente todas as compras
feitas enquanto o cartão estava ativo, e o faturamento de meses fechados cairia a
cada cancelamento.

Avaliando na data, a compra anterior continua somando no mês em que ocorreu, e o
cartão para de contribuir a partir do cancelamento.

`test_gold.py::TestCartaoCancelado`

## 15. Estorno abate o líquido, o bruto continua disponível

O fato expõe `valor_bruto`, `valor_estornado` e `valor_liquido`.

**Motivo:** zerar o valor da transação estornada perderia a informação de que ela
existiu, e a conciliação com a operadora usa o bruto. Guardar os três responde as
duas perguntas sem recalcular.

Estorno parcial abate só a parte estornada. Estornos múltiplos da mesma transação
são agregados antes do join, senão a transação viraria uma linha por estorno e o
bruto seria contado duas vezes.

`test_gold.py::TestEstorno`

## 16. Indicador de risco olha a transação inteira

`gold_indicadores_risco` não filtra por `elegivel_metrica`.

**Motivo:** fraude cometida com cartão que depois foi cancelado continua sendo
fraude, e provavelmente foi o cancelamento que respondeu à fraude. Excluí-la do
indicador esconderia justamente os casos mais graves.

`test_gold.py::TestIndicadoresRisco::test_fraude_em_cartao_cancelado_continua_no_indicador`

## 17. Features usam data de referência, não `current_date()`

**Motivo:** feature calculada com o relógio da máquina não é reproduzível. O mesmo
histórico gera valores diferentes conforme o dia em que o job roda, então o modelo
não pode ser reproduzido e a distribuição em produção não é a do treino.

Com data parametrizada, dá para gerar a base de treino de uma data passada
exatamente como ela era.

`test_pipeline.py::TestRegrasDeNegocioNaMassaCompleta::test_features_usam_a_data_de_referencia`

## 18. Ouro é reconstruída por overwrite

**Alternativa descartada:** MERGE incremental também na Ouro.

**Motivo:** a Ouro é derivada integralmente da Prata e não tem estado próprio. O
MERGE incremental resolve o problema de não perder o que já existe, e aqui não há
nada a perder: o resultado é função pura da Prata.

Vale enquanto o volume permitir recálculo completo. Quando não permitir, a mudança
é recalcular só as partições afetadas, não trocar por MERGE linha a linha.

## 19. Particionamento por data da transação

`fato_transacao` e `gold_fato_transacao` são particionados por `data_particao`.

**Alternativas descartadas:** particionar por `id_cliente` ou por data de
ingestão.

**Motivo:** consulta de fato transacional quase sempre filtra por período, então a
poda acontece sem esforço. Por cliente concentraria volume nos poucos de maior
movimento. Por data de ingestão faria a consulta por período varrer todas as
partições, já que dado atrasado cai em partição de outro dia.

Cada MERGE gera arquivos novos, então o pipeline chama `OPTIMIZE` ao fim das
camadas Prata e Ouro, com `ZORDER` nas colunas de filtro.

## 20. Falha interrompe a execução

O erro de uma etapa não é capturado. As métricas coletadas até ali são persistidas
em um bloco `finally`.

**Alternativa descartada:** capturar, registrar e seguir para a próxima etapa.

**Motivo:** uma Ouro construída sobre uma Prata que falhou no meio produz número
errado sem sinalizar nada, e alguém vai usar esse número. Não produzir resultado é
um problema visível; produzir resultado errado só aparece depois que a decisão foi
tomada.

## 21. Data de referência é data lógica e não filtra a carga

`data_referencia` alimenta `data_ingestao`, a janela de 30 dias e a recência das
features, e a coluna nas métricas. Nada no pipeline lê o relógio da máquina para
decidir valor de negócio.

Ela não filtra a carga: cada execução reprocessa a Bronze inteira, e é a
idempotência das camadas que garante que isso não duplique.

**Motivo:** reprocessar tudo é mais simples de raciocinar, não existe estado
parcial e uma correção de regra passa a valer para todo o histórico. Enquanto o
volume permitir, é a escolha certa.

Quando não couber na janela de execução, o caminho é filtrar a leitura da Bronze
por `data_ingestao` ou `batch_id`, recalcular só as partições afetadas na Ouro e
manter a dimensão fora disso, porque o SCD precisa da linha do tempo inteira.
