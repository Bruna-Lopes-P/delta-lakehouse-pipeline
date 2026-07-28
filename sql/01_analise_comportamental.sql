-- Análise comportamental na camada Ouro.
--
-- Todas as consultas rodam sobre gold_fato_transacao, cujo grão é uma linha por
-- transação, com o cadastro já resolvido para a versão vigente na data do fato.
--
-- Substitua {catalogo} e {schema} pelo ambiente. Em execução local sem Unity
-- Catalog, registre as tabelas com CREATE OR REPLACE TEMP VIEW apontando para o
-- caminho Delta, como no cabeçalho comentado abaixo.
--
--   CREATE OR REPLACE TEMP VIEW gold_fato_transacao
--   AS SELECT * FROM delta.`data/lakehouse/gold_dev/gold_fato_transacao`;


-- ---------------------------------------------------------------------------
-- 1. Detecção de anomalia de gasto
--
-- Compara o mês corrente do cliente com a média e o desvio dos seis meses
-- anteriores. O corte em dois desvios é a convenção usual para "fora do padrão"
-- em distribuicao aproximadamente normal.
--
-- A janela exclui o próprio mês (ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING).
-- Incluir o mês corrente na base de comparação contaminaria a referência: um
-- gasto atipico puxaria a própria media e deixaria de parecer atipico, que e
-- exatamente o caso que interessa detectar.
--
-- Clientes com menos de três meses de histórico ficam de fora. Desvio calculado
-- sobre dois pontos não sustenta conclusao.
-- ---------------------------------------------------------------------------

WITH movimento_mensal AS (
    SELECT
        id_cliente,
        ano_mes,
        COUNT(*)                     AS qtd_transacoes,
        SUM(valor_liquido)           AS valor_mes,
        AVG(valor_liquido)           AS ticket_medio,
        COUNT(DISTINCT mcc)          AS categorias_distintas,
        COUNT(DISTINCT data_particao) AS dias_ativos
    FROM gold_fato_transacao
    WHERE elegivel_metrica = true
    GROUP BY id_cliente, ano_mes
),

base_historica AS (
    SELECT
        id_cliente,
        ano_mes,
        qtd_transacoes,
        valor_mes,
        ticket_medio,
        categorias_distintas,
        AVG(valor_mes) OVER janela_anterior    AS media_6m,
        STDDEV(valor_mes) OVER janela_anterior AS desvio_6m,
        COUNT(*) OVER janela_anterior          AS meses_observados,
        LAG(valor_mes) OVER (
            PARTITION BY id_cliente ORDER BY ano_mes
        ) AS valor_mes_anterior,
        FIRST_VALUE(ano_mes) OVER (
            PARTITION BY id_cliente ORDER BY ano_mes
        ) AS primeiro_mes_ativo
    FROM movimento_mensal
    WINDOW janela_anterior AS (
        PARTITION BY id_cliente
        ORDER BY ano_mes
        ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
    )
),

classificacao AS (
    SELECT
        *,
        CASE
            WHEN meses_observados < 3 THEN 'HISTORICO_INSUFICIENTE'
            WHEN desvio_6m IS NULL OR desvio_6m = 0 THEN 'SEM_VARIACAO_HISTORICA'
            WHEN valor_mes > media_6m + 2 * desvio_6m THEN 'PICO_DE_GASTO'
            WHEN valor_mes < media_6m - 2 * desvio_6m THEN 'QUEDA_DE_GASTO'
            ELSE 'DENTRO_DO_PADRAO'
        END AS classificacao_anomalia,
        CASE
            WHEN media_6m > 0
            THEN ROUND((valor_mes - media_6m) / media_6m * 100, 2)
        END AS variacao_vs_media_pct,
        CASE
            WHEN desvio_6m > 0
            THEN ROUND((valor_mes - media_6m) / desvio_6m, 2)
        END AS desvios_da_media
    FROM base_historica
)

SELECT
    id_cliente,
    ano_mes,
    qtd_transacoes,
    ROUND(valor_mes, 2)  AS valor_mes,
    ROUND(media_6m, 2)   AS media_6m,
    ROUND(desvio_6m, 2)  AS desvio_6m,
    variacao_vs_media_pct,
    desvios_da_media,
    classificacao_anomalia
FROM classificacao
WHERE classificacao_anomalia IN ('PICO_DE_GASTO', 'QUEDA_DE_GASTO')
ORDER BY ABS(desvios_da_media) DESC, ano_mes DESC;


-- ---------------------------------------------------------------------------
-- 2. Cliente contra o próprio histórico e contra o grupo
--
-- Duas referências respondem perguntas diferentes. "Gastou mais do que costuma"
-- e comparação com o próprio passado. "Gasta muito para o perfil dele" e
-- comparação com quem tem o mesmo segmento e a mesma cidade.
--
-- O grupo e segmento e cidade juntos porque o poder de compra varia por praca:
-- comparar um cliente PREMIUM do interior com um PREMIUM da capital produziria
-- um desvio que reflete a praca, não o comportamento.
--
-- Os percentis usam PERCENTILE_CONT, que interpola entre os pontos vizinhos em
-- vez de devolver o valor observado mais próximo. Em grupo pequeno a diferença
-- entre os dois e visível.
-- ---------------------------------------------------------------------------

WITH movimento_cliente_mes AS (
    SELECT
        id_cliente,
        ano_mes,
        segmento_na_data AS segmento,
        cidade_na_data   AS cidade,
        estado_na_data   AS estado,
        COUNT(*)            AS qtd_transacoes,
        SUM(valor_liquido)  AS valor_mes,
        AVG(valor_liquido)  AS ticket_medio
    FROM gold_fato_transacao
    WHERE elegivel_metrica = true
    GROUP BY id_cliente, ano_mes, segmento_na_data, cidade_na_data, estado_na_data
),

referencia_propria AS (
    SELECT
        id_cliente,
        ano_mes,
        segmento,
        cidade,
        estado,
        qtd_transacoes,
        valor_mes,
        ticket_medio,
        AVG(valor_mes) OVER (
            PARTITION BY id_cliente ORDER BY ano_mes
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS media_propria_12m,
        LAG(valor_mes) OVER (PARTITION BY id_cliente ORDER BY ano_mes)  AS mes_anterior,
        LEAD(valor_mes) OVER (PARTITION BY id_cliente ORDER BY ano_mes) AS mes_seguinte,
        FIRST_VALUE(valor_mes) OVER (
            PARTITION BY id_cliente ORDER BY ano_mes
        ) AS primeiro_mes,
        LAST_VALUE(valor_mes) OVER (
            PARTITION BY id_cliente ORDER BY ano_mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ultimo_mes
    FROM movimento_cliente_mes
),

referencia_grupo AS (
    SELECT
        ano_mes,
        segmento,
        cidade,
        COUNT(DISTINCT id_cliente)                              AS clientes_no_grupo,
        AVG(valor_mes)                                          AS media_grupo,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY valor_mes) AS mediana_grupo,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY valor_mes) AS p75_grupo,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY valor_mes) AS p95_grupo
    FROM movimento_cliente_mes
    GROUP BY ano_mes, segmento, cidade
)

SELECT
    p.id_cliente,
    p.ano_mes,
    p.segmento,
    p.cidade,
    ROUND(p.valor_mes, 2)          AS valor_mes,

    -- Contra o próprio histórico
    ROUND(p.media_propria_12m, 2)  AS media_propria_12m,
    CASE
        WHEN p.media_propria_12m > 0
        THEN ROUND((p.valor_mes - p.media_propria_12m) / p.media_propria_12m * 100, 2)
    END AS variacao_vs_proprio_pct,
    CASE
        WHEN p.mes_anterior > 0
        THEN ROUND((p.valor_mes - p.mes_anterior) / p.mes_anterior * 100, 2)
    END AS variacao_vs_mes_anterior_pct,

    -- Contra o grupo
    g.clientes_no_grupo,
    ROUND(g.mediana_grupo, 2)      AS mediana_grupo,
    ROUND(g.p75_grupo, 2)          AS p75_grupo,
    ROUND(g.p95_grupo, 2)          AS p95_grupo,
    CASE
        WHEN g.mediana_grupo > 0
        THEN ROUND((p.valor_mes - g.mediana_grupo) / g.mediana_grupo * 100, 2)
    END AS variacao_vs_grupo_pct,

    CASE
        WHEN g.clientes_no_grupo < 5           THEN 'GRUPO_PEQUENO'
        WHEN p.valor_mes > g.p95_grupo         THEN 'MUITO_ACIMA_DO_GRUPO'
        WHEN p.valor_mes > g.p75_grupo         THEN 'ACIMA_DO_GRUPO'
        WHEN p.valor_mes < g.mediana_grupo     THEN 'ABAIXO_DO_GRUPO'
        ELSE 'NA_MEDIA_DO_GRUPO'
    END AS posicao_no_grupo

FROM referencia_propria p
JOIN referencia_grupo g
  ON p.ano_mes  = g.ano_mes
 AND p.segmento = g.segmento
 AND p.cidade   = g.cidade
ORDER BY p.ano_mes DESC, p.valor_mes DESC;


-- ---------------------------------------------------------------------------
-- 3. Segmentacao por valor e frequência
--
-- NTILE distribui os clientes em faixas de tamanho igual; PERCENT_RANK devolve
-- a posição relativa continua. Os dois respondem coisas diferentes: NTILE serve
-- para ação operacional ("tratar o quartil superior"), PERCENT_RANK serve para
-- comparação entre bases de tamanhos distintos.
--
-- A matriz valor x frequência separa dois perfis que o valor sozinho confunde:
-- quem gasta muito em poucas compras grandes e quem gasta o mesmo em muitas
-- compras pequenas. A ação comercial para os dois não é a mesma.
-- ---------------------------------------------------------------------------

WITH consolidado_cliente AS (
    SELECT
        id_cliente,
        MAX(segmento_na_data)         AS segmento,
        MAX(cidade_na_data)           AS cidade,
        COUNT(*)                      AS qtd_transacoes,
        SUM(valor_liquido)            AS valor_total,
        AVG(valor_liquido)            AS ticket_medio,
        COUNT(DISTINCT data_particao) AS dias_ativos,
        COUNT(DISTINCT ano_mes)       AS meses_ativos,
        MAX(data_particao)            AS ultima_compra,
        MIN(data_particao)            AS primeira_compra
    FROM gold_fato_transacao
    WHERE elegivel_metrica = true
    GROUP BY id_cliente
),

segmentado AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY valor_total)     AS quartil_valor,
        NTILE(4) OVER (ORDER BY qtd_transacoes)  AS quartil_frequencia,
        NTILE(10) OVER (ORDER BY valor_total)    AS decil_valor,
        ROUND(PERCENT_RANK() OVER (ORDER BY valor_total), 4)    AS percentil_valor,
        ROUND(PERCENT_RANK() OVER (ORDER BY qtd_transacoes), 4) AS percentil_frequencia,
        ROUND(CUME_DIST() OVER (ORDER BY valor_total), 4)       AS distribuicao_acumulada
    FROM consolidado_cliente
)

SELECT
    id_cliente,
    segmento,
    cidade,
    qtd_transacoes,
    ROUND(valor_total, 2) AS valor_total,
    ROUND(ticket_medio, 2) AS ticket_medio,
    meses_ativos,
    quartil_valor,
    quartil_frequencia,
    decil_valor,
    percentil_valor,
    CASE
        WHEN quartil_valor = 4 AND quartil_frequencia = 4 THEN 'ALTO_VALOR_ALTA_FREQUENCIA'
        WHEN quartil_valor = 4 AND quartil_frequencia <= 2 THEN 'ALTO_VALOR_BAIXA_FREQUENCIA'
        WHEN quartil_valor <= 2 AND quartil_frequencia = 4 THEN 'BAIXO_VALOR_ALTA_FREQUENCIA'
        WHEN quartil_valor = 1 AND quartil_frequencia = 1 THEN 'BAIXO_ENGAJAMENTO'
        ELSE 'INTERMEDIARIO'
    END AS perfil_valor_frequencia
FROM segmentado
ORDER BY valor_total DESC;


-- ---------------------------------------------------------------------------
-- 4. Curva ABC de estabelecimento
--
-- Ordena por valor e acumula o percentual até identificar onde esta a
-- concentração. A soma acumulada usa a mesma ordenação da soma total, e o
-- default da window frame (RANGE UNBOUNDED PRECEDING) já produz o acumulado
-- correto quando existe ORDER BY.
-- ---------------------------------------------------------------------------

WITH movimento_estabelecimento AS (
    SELECT
        estabelecimento,
        mcc,
        COUNT(*)                       AS qtd_transacoes,
        SUM(valor_liquido)             AS valor_total,
        AVG(valor_liquido)             AS ticket_medio,
        COUNT(DISTINCT id_cliente)     AS clientes_distintos,
        SUM(CASE WHEN foi_estornada THEN 1 ELSE 0 END) AS qtd_estornadas
    FROM gold_fato_transacao
    GROUP BY estabelecimento, mcc
),

acumulado AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY valor_total DESC) AS posicao,
        SUM(valor_total) OVER ()                      AS valor_geral,
        SUM(valor_total) OVER (ORDER BY valor_total DESC) AS valor_acumulado
    FROM movimento_estabelecimento
)

SELECT
    posicao,
    estabelecimento,
    mcc,
    qtd_transacoes,
    clientes_distintos,
    ROUND(valor_total, 2)  AS valor_total,
    ROUND(ticket_medio, 2) AS ticket_medio,
    ROUND(100.0 * valor_total / valor_geral, 2)     AS participacao_pct,
    ROUND(100.0 * valor_acumulado / valor_geral, 2) AS participacao_acumulada_pct,
    ROUND(100.0 * qtd_estornadas / qtd_transacoes, 2) AS taxa_estorno_pct,
    CASE
        WHEN 100.0 * valor_acumulado / valor_geral <= 80 THEN 'A'
        WHEN 100.0 * valor_acumulado / valor_geral <= 95 THEN 'B'
        ELSE 'C'
    END AS classe_abc
FROM acumulado
ORDER BY posicao;
