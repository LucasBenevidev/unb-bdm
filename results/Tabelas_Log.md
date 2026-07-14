# Tabelas de Log e Auditoria - Data Warehouse SIORG

Este documento detalha a estrutura, o significado das colunas e a utilidade científica das duas tabelas de metadados criadas no ClickHouse: **`log_cargas`** e **`log_consultas`**. 

Elas servem para fins de auditoria de operações de engenharia de dados (ingestão) e análises de desempenho sob concorrência e estresse.

---

## 1. Tabela `log_cargas` (Logs de Ingestão do S3)

A tabela `log_cargas` armazena o histórico e as estatísticas de cada arquivo CSV carregado a partir do bucket AWS S3 para dentro do ClickHouse.

### Estrutura DDL (ClickHouse):
```sql
CREATE TABLE IF NOT EXISTS log_cargas (
    data_execucao DateTime DEFAULT now(),
    id_execucao UInt32,
    nome_arquivo String,
    tempo_carga Float64,
    total_linhas UInt64,
    linhas_por_segundo Float64
) ENGINE = MergeTree()
ORDER BY data_execucao;
```

### Significado das Colunas:
| Coluna | Tipo ClickHouse | Descrição |
| :--- | :--- | :--- |
| `data_execucao` | `DateTime` | Data e hora exatas de quando a ingestão do arquivo foi finalizada (padrão: `now()`). |
| `id_execucao` | `UInt32` | Número identificador sequencial único da execução do pipeline. Todos os arquivos ingeridos em uma mesma rodada de Job compartilham o mesmo ID, facilitando agrupamentos. |
| `nome_arquivo` | `String` | Nome do arquivo CSV de origem importado do S3 (ex: `distribuicao-orgaos-siorg-2025-04.csv`). |
| `tempo_carga` | `Float64` | Tempo total de carregamento e processamento interno do arquivo medido em segundos. |
| `total_linhas` | `UInt64` | Contagem total de registros lidos e importados com sucesso para a respectiva tabela do DWH. |
| `linhas_por_segundo`| `Float64` | Velocidade líquida de processamento (linhas divididas pelo tempo de carga). |

### Utilidade Científica e Operacional:
* **Detecção de Gargalos na Rede:** Permite plotar gráficos da velocidade de ingestão (`linhas_por_segundo`) ao longo do tempo para detectar flutuações de largura de banda na conexão nativa S3 $\leftrightarrow$ ClickHouse Cloud.
* **Auditoria de Carga:** Garante a rastreabilidade completa para comprovar quais snapshots históricos de dados estão efetivamente consolidados no armazém analítico.

---

## 2. Tabela `log_consultas` (Logs de Benchmark e Concorrência)

A tabela `log_consultas` armazena o perfil de hardware e as métricas de tempo de resposta das consultas analíticas executadas sob concorrência.

### Estrutura DDL (ClickHouse):
```sql
CREATE TABLE IF NOT EXISTS log_consultas (
    data_execucao DateTime DEFAULT now(),
    id_execucao UInt32,
    concurrency_level UInt8,
    query_name String,
    query_id String,
    elapsed_seconds Float64,
    read_rows UInt64,
    read_bytes UInt64,
    result_rows UInt64,
    query_text String
) ENGINE = MergeTree()
ORDER BY data_execucao;
```

### Significado das Colunas:
| Coluna | Tipo ClickHouse | Descrição |
| :--- | :--- | :--- |
| `data_execucao` | `DateTime` | Data e hora exatas de término da execução da consulta analítica. |
| `id_execucao` | `UInt32` | Identificador da rodada do benchmark, agrupando as diferentes métricas daquele experimento de concorrência. |
| `concurrency_level` | `UInt8` | Nível de concorrência testado (número de threads/usuários fazendo consultas idênticas paralelas: 1, 5, 10 ou 20). |
| `query_name` | `String` | Nome/rótulo identificador amigável da consulta científica (ex: *Query 2: Relocalizações Geográficas*). |
| `query_id` | `String` | ID interno único gerado pelo mecanismo do ClickHouse Cloud para aquela transação específica. |
| `elapsed_seconds` | `Float64` | Tempo total medido em nível de engine do ClickHouse (sem latência de rede externa) em segundos. |
| `read_rows` | `UInt64` | Quantidade de registros escaneados da tabela pelo motor analítico para computar a resposta. |
| `read_bytes` | `UInt64` | Volume de dados brutos carregados do armazenamento/memória para processar a query (em bytes). |
| `result_rows` | `UInt64` | O tamanho total do conjunto de resultados (número de linhas retornadas). |
| `query_text` | `String` | O script SQL exato que foi submetido ao banco. |

### Utilidade Científica e Operacional:
* **Estudos de Escalabilidade de Banco:** Possibilita fazer análises estatísticas e gráficos de dispersão mostrando o crescimento do tempo de resposta médio (`elapsed_seconds`) em relação ao aumento de usuários concorrentes (`concurrency_level`), validando o limite físico de processamento do hardware.
* **Otimização de Índices:** Ajuda a analisar a eficiência de chaves de ordenação primárias comparando o volume escaneado de dados (`read_bytes`) e linhas (`read_rows`) entre diferentes variações de chaves de busca.

---

## 3. Consultas Úteis para Análise Científica dos Logs

Aqui estão alguns exemplos de queries SQL úteis para analisar os metadados diretamente no ClickHouse:

### A. Velocidade média de processamento do pipeline por rodada de Job:
```sql
SELECT 
    id_execucao,
    count(nome_arquivo) AS total_arquivos,
    round(sum(total_linhas) / 1000000, 2) AS total_milhoes_linhas,
    round(avg(linhas_por_segundo), 2) AS media_linhas_por_segundo,
    round(sum(tempo_carga), 2) AS tempo_total_segundos
FROM SIORG.log_cargas
GROUP BY id_execucao
ORDER BY id_execucao DESC;
```

### B. Escalabilidade de concorrência por tipo de Query:
```sql
SELECT 
    query_name,
    concurrency_level,
    count(query_id) AS total_execucoes,
    round(avg(elapsed_seconds), 4) AS tempo_medio_segundos,
    round(avg(read_rows) / 1000000, 2) AS media_milhoes_linhas_escaneadas,
    round(avg(read_bytes) / (1024*1024), 2) AS media_dados_escaneados_mb
FROM SIORG.log_consultas
GROUP BY query_name, concurrency_level
ORDER BY query_name, concurrency_level;
```
