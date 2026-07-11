# Consultas Analíticas - Sistema SIORG

Este documento descreve as três principais consultas SQL analíticas projetadas para monitorar a estrutura administrativa do Poder Executivo Federal brasileiro utilizando os dados do sistema SIORG armazenados no data warehouse ClickHouse.

---

## Consulta 1: Monitoramento da Expansão e Retração do Estado (Série Temporal)

Esta consulta acompanha a evolução mensal do tamanho físico da máquina pública, contando o total de setores ativos, o total de órgãos (Ministérios, Autarquias, Fundações) e a variação líquida em relação ao mês anterior.

### SQL (exatamente como implementado no benchmark):
```sql
SELECT
    ano_referencia,
    mes_referencia,
    count(DISTINCT codigoUnidade) AS total_unidades,
    count(DISTINCT codigoOrgaoEntidade) AS total_orgaos,
    total_unidades - lagInFrame(total_unidades, 1, 0) OVER (ORDER BY ano_referencia, mes_referencia) AS variacao_liquida_unidades
FROM estrutura_organizacional_completa
GROUP BY ano_referencia, mes_referencia
ORDER BY ano_referencia, mes_referencia
```

### Utilidade Prática:
* **Para Gestores Públicos:** 
  Permite analisar o ritmo de crescimento e consolidação da estrutura federal. Facilita o controle orçamentário e a análise de impactos de decretos de reforma administrativa (como fusões de ministérios ou corte de cargos/estruturas).
* **Para a Sociedade Civil:** 
  Aumenta a transparência sobre o tamanho do Estado, permitindo avaliar cientificamente se a estrutura governamental está expandindo (criando mais divisões e superintendências) ou encolhendo ao longo do tempo.

---

## Consulta 2: Rastreamento de Relocalizações Geográficas de Setores (Self-Join Histórico)

Esta consulta identifica quais setores públicos (`cod_unidade`) mudaram fisicamente de município ou de estado (UF) de um mês para o outro na série histórica.

### SQL (exatamente como implementado no benchmark):
```sql
SELECT
    A.ano_referencia AS ano_anterior,
    A.mes_referencia AS mes_anterior,
    B.ano_referencia AS ano_atual,
    B.mes_referencia AS mes_atual,
    A.nome_orgao_entidade,
    A.nome_unidade,
    A.municipio AS municipio_origem,
    A.uf AS uf_origem,
    B.municipio AS municipio_destino,
    B.uf AS uf_destino
FROM distribuicao_orgaos AS A
INNER JOIN distribuicao_orgaos AS B 
    ON A.cod_unidade = B.cod_unidade
   AND B.ano_referencia = (A.mes_referencia == 12 ? A.ano_referencia + 1 : A.ano_referencia)
   AND B.mes_referencia = (A.mes_referencia == 12 ? 1 : A.mes_referencia + 1)
WHERE A.municipio != B.municipio OR A.uf != B.uf
ORDER BY A.nome_orgao_entidade, A.ano_referencia, A.mes_referencia
```

### Utilidade Prática:
* **Para Gestores Públicos:** 
  Ajuda a monitorar a interiorização ou a centralização física de serviços públicos. Permite verificar o cumprimento de políticas de descentralização e planejar as necessidades de infraestrutura logística de servidores transferidos.
* **Para a Sociedade Civil:** 
  Permite auditar a migração geográfica do atendimento público. Ajuda a identificar se os serviços essenciais estão se afastando de regiões periféricas/interioranas em direção às capitais ou distritos centrais.

---

## Consulta 3: Série Histórica de Complexidade e Burocracia Organizacional (Agrupamento por Órgão)

Esta consulta quantifica a complexidade interna de cada órgão do governo mês a mês. Ela calcula o total de setores ativos, a profundidade máxima da hierarquia de subordinação (número de níveis de chefe a subordinado) e a média do nível hierárquico.

### SQL (exatamente como implementado no benchmark):
```sql
SELECT
    nome_orgao_entidade,
    sigla_orgao_entidade,
    ano_referencia,
    mes_referencia,
    count(DISTINCT cod_unidade) AS total_setores,
    max(toUInt8OrZero(nivel_hierarquico)) AS profundidade_maxima_hierarquia,
    round(avg(toUInt8OrZero(nivel_hierarquico)), 2) AS nivel_hierarquico_medio
FROM distribuicao_orgaos
WHERE nome_orgao_entidade IS NOT NULL 
  AND nome_orgao_entidade != ''
GROUP BY 
    nome_orgao_entidade, 
    sigla_orgao_entidade, 
    ano_referencia, 
    mes_referencia
ORDER BY 
    nome_orgao_entidade, 
    ano_referencia, 
    mes_referencia
```

### Utilidade Prática:
* **Para Gestores Públicos:** 
  Serve como indicador direto de burocracia e velocidade de tomada de decisão. Órgãos com hierarquia muito profunda (`profundidade_maxima_hierarquia` alta) e nível médio alto tendem a ter linhas de comunicação mais lentas e maior número de cargos de chefia intermediária, servindo como alvo primário para programas de desburocratização e simplificação de processos.
* **Para a Sociedade Civil:** 
  Permite criar rankings de órgãos governamentais com as estruturas mais complexas e pesadas da administração, possibilitando a cobrança por eficiência de gestão e redução de cargos de assessoramento puramente burocráticos.
