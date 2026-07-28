-- Monitoramento do pipeline.
--
-- As consultas abaixo são a base do que viraria alerta em produção. Todas leem
-- observabilidade.execucao_pipeline e quarentena.registros_rejeitados.


-- ---------------------------------------------------------------------------
-- 1. Resumo da última execução
-- ---------------------------------------------------------------------------

-- QUALIFY resolveria isso em uma linha, mas é extensão do Databricks SQL e não
-- existe no Spark open source. A subquery com ROW_NUMBER e equivalente e roda
-- nos dois, o que importa porque o pipeline também e executado fora do
-- Databricks na suite de testes.
WITH ultima_execucao AS (
    SELECT batch_id FROM (
        SELECT
            batch_id,
            ROW_NUMBER() OVER (ORDER BY iniciado_em DESC) AS ordem
        FROM execucao_pipeline
    ) WHERE ordem = 1
)

SELECT
    m.camada,
    m.etapa,
    m.entidade,
    m.linhas_lidas,
    m.linhas_gravadas,
    m.linhas_rejeitadas,
    CASE
        WHEN m.linhas_lidas > 0
        THEN ROUND(100.0 * m.linhas_rejeitadas / m.linhas_lidas, 2)
    END AS taxa_rejeicao_pct,
    m.duracao_segundos,
    m.status,
    m.detalhe_erro
FROM execucao_pipeline m
JOIN ultima_execucao u ON m.batch_id = u.batch_id
ORDER BY m.iniciado_em;


-- ---------------------------------------------------------------------------
-- 2. Queda de volume contra a media movel
--
-- Alerta operacional mais útil que limite fixo: o volume normal de uma entidade
-- muda com o tempo, e limite fixo ou dispara toda semana ou nunca dispara.
--
-- A janela exclui o dia corrente (ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING).
-- Incluir o próprio dia na base de comparação amorteceria justamente a anomalia
-- que se quer detectar.
-- ---------------------------------------------------------------------------

WITH volume_diario AS (
    SELECT
        data_referencia,
        camada,
        entidade,
        SUM(linhas_gravadas) AS linhas_gravadas
    FROM execucao_pipeline
    WHERE status = 'SUCESSO'
    GROUP BY data_referencia, camada, entidade
),

com_referencia AS (
    SELECT
        *,
        AVG(linhas_gravadas) OVER janela   AS media_7d,
        STDDEV(linhas_gravadas) OVER janela AS desvio_7d,
        COUNT(*) OVER janela               AS dias_observados
    FROM volume_diario
    WINDOW janela AS (
        PARTITION BY camada, entidade
        ORDER BY data_referencia
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    )
)

SELECT
    data_referencia,
    camada,
    entidade,
    linhas_gravadas,
    ROUND(media_7d, 0) AS media_7d,
    CASE
        WHEN media_7d > 0
        THEN ROUND((linhas_gravadas - media_7d) / media_7d * 100, 2)
    END AS variacao_pct,
    CASE
        WHEN dias_observados < 3                       THEN 'HISTORICO_INSUFICIENTE'
        WHEN linhas_gravadas = 0                       THEN 'CRITICO_SEM_DADO'
        WHEN linhas_gravadas < media_7d * 0.5          THEN 'CRITICO_QUEDA_ACENTUADA'
        WHEN linhas_gravadas < media_7d * 0.8          THEN 'ATENCAO_QUEDA'
        WHEN linhas_gravadas > media_7d * 2            THEN 'ATENCAO_PICO'
        ELSE 'NORMAL'
    END AS alerta
FROM com_referencia
WHERE dias_observados >= 3
ORDER BY data_referencia DESC, camada, entidade;


-- ---------------------------------------------------------------------------
-- 3. Taxa de rejeição por entidade ao longo do tempo
--
-- Subida na taxa costuma indicar mudanca na origem, não no pipeline. Serve
-- para abrir conversa com quem produz o dado.
-- ---------------------------------------------------------------------------

SELECT
    data_referencia,
    entidade,
    SUM(linhas_lidas)      AS lidas,
    SUM(linhas_rejeitadas) AS rejeitadas,
    ROUND(100.0 * SUM(linhas_rejeitadas) / NULLIF(SUM(linhas_lidas), 0), 2) AS taxa_pct,
    LAG(ROUND(100.0 * SUM(linhas_rejeitadas) / NULLIF(SUM(linhas_lidas), 0), 2)) OVER (
        PARTITION BY entidade ORDER BY data_referencia
    ) AS taxa_dia_anterior
FROM execucao_pipeline
WHERE camada = 'silver'
GROUP BY data_referencia, entidade
ORDER BY data_referencia DESC, taxa_pct DESC;


-- ---------------------------------------------------------------------------
-- 4. Motivos de rejeição mais frequentes
--
-- Direciona a correção: um motivo concentrando a maior parte das rejeições
-- geralmente é um problema único na origem, não dado ruim disperso.
-- ---------------------------------------------------------------------------

WITH motivos AS (
    SELECT
        data_referencia,
        entidade,
        EXPLODE(motivos_rejeicao) AS motivo
    FROM registros_rejeitados
)

SELECT
    entidade,
    motivo,
    COUNT(*) AS ocorrencias,
    COUNT(DISTINCT data_referencia) AS dias_com_ocorrencia,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY entidade), 2) AS pct_da_entidade,
    MIN(data_referencia) AS primeira_ocorrencia,
    MAX(data_referencia) AS ultima_ocorrencia
FROM motivos
GROUP BY entidade, motivo
ORDER BY ocorrencias DESC;


-- ---------------------------------------------------------------------------
-- 5. Duracao das etapas contra o próprio histórico
--
-- Degradacao de performance costuma ser gradual. Comparar contra a mediana das
-- últimas execuções pega a tendencia antes de virar incidente.
-- ---------------------------------------------------------------------------

WITH historico_duracao AS (
    SELECT
        camada,
        etapa,
        entidade,
        data_referencia,
        duracao_segundos,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duracao_segundos) OVER (
            PARTITION BY camada, etapa, entidade
        ) AS mediana_geral
    FROM execucao_pipeline
    WHERE status = 'SUCESSO'
)

SELECT
    camada,
    etapa,
    entidade,
    data_referencia,
    duracao_segundos,
    ROUND(mediana_geral, 1) AS mediana_historica,
    CASE
        WHEN mediana_geral > 0
        THEN ROUND(duracao_segundos / mediana_geral, 2)
    END AS razao_vs_mediana,
    CASE
        WHEN duracao_segundos > mediana_geral * 3 THEN 'DEGRADACAO_SEVERA'
        WHEN duracao_segundos > mediana_geral * 2 THEN 'DEGRADACAO'
        ELSE 'NORMAL'
    END AS alerta_performance
FROM historico_duracao
ORDER BY razao_vs_mediana DESC NULLS LAST;


-- ---------------------------------------------------------------------------
-- 6. Frescor do dado
--
-- Responde "até quando o dado está atualizado", que é a pergunta que chega da
-- área de negocio quando um número parece errado.
-- ---------------------------------------------------------------------------

SELECT
    'gold_fato_transacao' AS tabela,
    MAX(data_particao)    AS ultima_particao,
    DATEDIFF(CURRENT_DATE(), MAX(data_particao)) AS dias_de_atraso,
    COUNT(*)              AS linhas,
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), MAX(data_particao)) > 2 THEN 'CRITICO'
        WHEN DATEDIFF(CURRENT_DATE(), MAX(data_particao)) > 1 THEN 'ATENCAO'
        ELSE 'OK'
    END AS status_frescor
FROM gold_fato_transacao;


-- ---------------------------------------------------------------------------
-- 7. Execuções com falha
-- ---------------------------------------------------------------------------

SELECT
    batch_id,
    data_referencia,
    camada,
    etapa,
    entidade,
    detalhe_erro,
    iniciado_em
FROM execucao_pipeline
WHERE status = 'FALHA'
ORDER BY iniciado_em DESC;
