-- SCD Tipo 2: consulta temporal e carga incremental.
--
-- As consultas de leitura funcionam em qualquer engine com suporte a window
-- function. O MERGE ao final exige Delta Lake.


-- ---------------------------------------------------------------------------
-- 1. Cadastro vigente em uma data
--
-- A consulta central do SCD Tipo 2. O intervalo e fechado no início e aberto no
-- fim, então no instante exato do corte vale a versão nova e nenhuma linha
-- responde duas vezes.
-- ---------------------------------------------------------------------------

SELECT
    id_cliente,
    nome,
    cidade,
    estado,
    segmento,
    renda,
    dw_inicio_vigencia,
    dw_fim_vigencia
FROM dim_cliente
WHERE dw_inicio_vigencia <= TIMESTAMP '2026-02-20 00:00:00'
  AND (dw_fim_vigencia IS NULL OR dw_fim_vigencia > TIMESTAMP '2026-02-20 00:00:00')
  AND NOT dw_excluido
ORDER BY id_cliente;


-- ---------------------------------------------------------------------------
-- 2. Linha do tempo de uma entidade
--
-- Mostra o que mudou entre versões consecutivas. LAG traz o valor anterior, o
-- que evita o self join que seria necessário sem window function.
--
-- LEAD sobre dw_inicio_vigencia serve de verificacao: precisa coincidir com
-- dw_fim_vigencia em toda linha. Divergência indica buraco na linha do tempo.
-- ---------------------------------------------------------------------------

WITH historico AS (
    SELECT
        id_cliente,
        nome,
        cidade,
        segmento,
        renda,
        dw_inicio_vigencia,
        dw_fim_vigencia,
        dw_versao_ativa,
        dw_excluido,
        ROW_NUMBER() OVER (
            PARTITION BY id_cliente ORDER BY dw_inicio_vigencia
        ) AS versao,
        LAG(segmento) OVER (PARTITION BY id_cliente ORDER BY dw_inicio_vigencia) AS segmento_anterior,
        LAG(cidade)   OVER (PARTITION BY id_cliente ORDER BY dw_inicio_vigencia) AS cidade_anterior,
        LAG(renda)    OVER (PARTITION BY id_cliente ORDER BY dw_inicio_vigencia) AS renda_anterior,
        LEAD(dw_inicio_vigencia) OVER (
            PARTITION BY id_cliente ORDER BY dw_inicio_vigencia
        ) AS inicio_da_proxima,
        FIRST_VALUE(dw_inicio_vigencia) OVER (
            PARTITION BY id_cliente ORDER BY dw_inicio_vigencia
        ) AS primeira_vigencia,
        LAST_VALUE(segmento) OVER (
            PARTITION BY id_cliente ORDER BY dw_inicio_vigencia
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS segmento_atual
    FROM dim_cliente
)

SELECT
    id_cliente,
    versao,
    dw_inicio_vigencia,
    dw_fim_vigencia,
    dw_versao_ativa,
    segmento_anterior,
    segmento,
    CASE WHEN segmento IS DISTINCT FROM segmento_anterior THEN 'SIM' ELSE 'NAO' END AS mudou_segmento,
    CASE WHEN cidade   IS DISTINCT FROM cidade_anterior   THEN 'SIM' ELSE 'NAO' END AS mudou_cidade,
    CASE
        WHEN renda_anterior > 0
        THEN ROUND((renda - renda_anterior) / renda_anterior * 100, 2)
    END AS variacao_renda_pct,
    DATEDIFF(COALESCE(dw_fim_vigencia, CURRENT_TIMESTAMP()), dw_inicio_vigencia) AS dias_de_vigencia,
    segmento_atual,
    CASE
        WHEN dw_fim_vigencia IS DISTINCT FROM inicio_da_proxima THEN 'INCONSISTENTE'
        ELSE 'OK'
    END AS consistencia_da_linha_do_tempo
FROM historico
ORDER BY id_cliente, versao;


-- ---------------------------------------------------------------------------
-- 3. Verificacao das invariantes do SCD
--
-- Três condições precisam valer sempre. Qualquer linha retornada aqui é defeito.
-- ---------------------------------------------------------------------------

-- 3a. No maximo uma versão ativa por chave
SELECT id_cliente, COUNT(*) AS versoes_ativas
FROM dim_cliente
WHERE dw_versao_ativa
GROUP BY id_cliente
HAVING COUNT(*) > 1;

-- 3b. Intervalo invertido
SELECT id_cliente, dw_inicio_vigencia, dw_fim_vigencia
FROM dim_cliente
WHERE dw_fim_vigencia IS NOT NULL
  AND dw_fim_vigencia <= dw_inicio_vigencia;

-- 3c. Buraco ou sobreposição entre versões consecutivas
WITH encadeamento AS (
    SELECT
        id_cliente,
        dw_inicio_vigencia,
        dw_fim_vigencia,
        LEAD(dw_inicio_vigencia) OVER (
            PARTITION BY id_cliente ORDER BY dw_inicio_vigencia
        ) AS inicio_da_proxima
    FROM dim_cliente
)
SELECT *
FROM encadeamento
WHERE inicio_da_proxima IS NOT NULL
  AND dw_fim_vigencia IS DISTINCT FROM inicio_da_proxima;


-- ---------------------------------------------------------------------------
-- 4. MERGE do SCD Tipo 2 em SQL
--
-- Equivale ao que src/silver/scd.py faz em PySpark.
--
-- A origem emite duas linhas por alteração. A de _chave_merge preenchida casa
-- com a versão vigente e a fecha; a de _chave_merge nula não casa e é inserida.
-- Um MERGE sozinho não consegue fazer as duas coisas, porque cada linha da
-- origem dispara no maximo uma ação.
--
-- O filtro por hash é o que dá idempotência: sem ele, reprocessar o mesmo
-- arquivo abriria uma versão identica a anterior.
-- ---------------------------------------------------------------------------

WITH origem_preparada AS (
    SELECT
        id_cliente,
        nome,
        cidade,
        estado,
        renda,
        segmento,
        data_atualizacao,
        operacao,
        SHA2(CONCAT_WS('||',
            COALESCE(nome, '<nulo>'),
            COALESCE(cidade, '<nulo>'),
            COALESCE(estado, '<nulo>'),
            COALESCE(CAST(renda AS STRING), '<nulo>'),
            COALESCE(segmento, '<nulo>')
        ), 256) AS dw_hash_atributos
    FROM stg_cliente
),

vigente AS (
    SELECT id_cliente, dw_hash_atributos, dw_inicio_vigencia
    FROM dim_cliente
    WHERE dw_versao_ativa
),

aplicaveis AS (
    SELECT o.*
    FROM origem_preparada o
    LEFT JOIN vigente v ON o.id_cliente = v.id_cliente
    WHERE v.id_cliente IS NULL                                     -- entidade nova
       OR (o.data_atualizacao >= v.dw_inicio_vigencia              -- nao e atrasado
           AND (o.dw_hash_atributos <> v.dw_hash_atributos         -- mudou de fato
                OR o.operacao = 'DELETE'))
),

staged AS (
    -- Linha que fecha a versão vigente
    SELECT id_cliente AS _chave_merge, * FROM aplicaveis
    UNION ALL
    -- Linha que insere a versão nova
    SELECT CAST(NULL AS BIGINT) AS _chave_merge, * FROM aplicaveis
)

MERGE INTO dim_cliente AS alvo
USING staged AS origem
   ON alvo.id_cliente = origem._chave_merge
  AND alvo.dw_versao_ativa = true

WHEN MATCHED THEN UPDATE SET
    alvo.dw_versao_ativa   = false,
    alvo.dw_fim_vigencia   = origem.data_atualizacao,
    alvo.dw_atualizado_em  = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    id_cliente, nome, cidade, estado, renda, segmento, data_atualizacao,
    dw_hash_atributos, dw_inicio_vigencia, dw_fim_vigencia,
    dw_versao_ativa, dw_excluido, dw_atualizado_em
) VALUES (
    origem.id_cliente, origem.nome, origem.cidade, origem.estado, origem.renda,
    origem.segmento, origem.data_atualizacao,
    origem.dw_hash_atributos, origem.data_atualizacao, NULL,
    origem.operacao <> 'DELETE', origem.operacao = 'DELETE', CURRENT_TIMESTAMP()
);


-- ---------------------------------------------------------------------------
-- 5. MERGE de fato: deduplicação por chave natural
--
-- Fato não versiona. A mesma transação pode chegar em dois arquivos quando a
-- origem reenvia uma janela; vence a ingestão mais recente.
--
-- Deduplicar antes do MERGE não é opcional: com a chave repetida na origem, o
-- Delta aborta com erro de múltiplas linhas casando o mesmo alvo.
-- ---------------------------------------------------------------------------

WITH origem_deduplicada AS (
    SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id_transacao ORDER BY timestamp_ingestao DESC
            ) AS ordem
        FROM stg_transacao
    ) WHERE ordem = 1
)

MERGE INTO fato_transacao AS alvo
USING origem_deduplicada AS origem
   ON alvo.id_transacao = origem.id_transacao
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
