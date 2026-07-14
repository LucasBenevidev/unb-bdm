SELECT
    a.ano_referencia AS ano_anterior,
    a.mes_referencia AS mes_anterior,
    b.ano_referencia AS ano_atual,
    b.mes_referencia AS mes_atual,
    a.nome_orgao_entidade,
    a.nome_unidade,
    a.municipio AS municipio_origem,
    a.uf AS uf_origem,
    b.municipio AS municipio_destino,
    b.uf AS uf_destino
FROM iceberg.siorg.distribuicao_orgaos AS a
INNER JOIN iceberg.siorg.distribuicao_orgaos AS b
    ON a.cod_unidade = b.cod_unidade
   AND b.ano_referencia = CASE WHEN a.mes_referencia = 12 THEN a.ano_referencia + 1 ELSE a.ano_referencia END
   AND b.mes_referencia = CASE WHEN a.mes_referencia = 12 THEN 1 ELSE a.mes_referencia + 1 END
WHERE a.municipio <> b.municipio OR a.uf <> b.uf
ORDER BY a.nome_orgao_entidade, a.ano_referencia, a.mes_referencia

