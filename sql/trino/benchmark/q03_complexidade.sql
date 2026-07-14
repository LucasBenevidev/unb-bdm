SELECT
    nome_orgao_entidade,
    sigla_orgao_entidade,
    ano_referencia,
    mes_referencia,
    count(DISTINCT cod_unidade) AS total_setores,
    max(COALESCE(try_cast(nivel_hierarquico AS integer), 0)) AS profundidade_maxima_hierarquia,
    round(avg(COALESCE(try_cast(nivel_hierarquico AS integer), 0)), 2) AS nivel_hierarquico_medio
FROM iceberg.siorg.distribuicao_orgaos
WHERE nome_orgao_entidade IS NOT NULL
  AND nome_orgao_entidade <> ''
GROUP BY
    nome_orgao_entidade,
    sigla_orgao_entidade,
    ano_referencia,
    mes_referencia
ORDER BY
    nome_orgao_entidade,
    ano_referencia,
    mes_referencia

