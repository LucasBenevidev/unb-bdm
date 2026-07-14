SELECT
    ano_referencia,
    mes_referencia,
    count(DISTINCT "codigoUnidade") AS total_unidades,
    count(DISTINCT "codigoOrgaoEntidade") AS total_orgaos,
    count(DISTINCT "codigoUnidade")
        - lag(count(DISTINCT "codigoUnidade"), 1, 0)
          OVER (ORDER BY ano_referencia, mes_referencia) AS variacao_liquida_unidades
FROM iceberg.siorg.estrutura_organizacional_completa
GROUP BY ano_referencia, mes_referencia
ORDER BY ano_referencia, mes_referencia

