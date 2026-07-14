# Trino + Apache Iceberg + AWS Glue + S3

Implementacao da trilha Trino/Iceberg para comparar com a trilha ClickHouse ja existente.

## Arquitetura

`Trino local (Docker) -> AWS Glue Data Catalog -> Apache Iceberg -> Amazon S3`

Catalogo: `iceberg`  
Schema: `iceberg.siorg`  
Warehouse/schema location: `s3://unb-bdm-siorg/iceberg/siorg/`

## Pre-requisitos

- Docker com `docker compose`
- Python 3.11+
- Dependencias de `requirements.txt`
- `.env` local com credenciais AWS e configuracao Trino

Exemplo de `.env`:

```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=unb-bdm-siorg
TRINO_HOST=localhost
TRINO_PORT=8090
TRINO_USER=cassio
TRINO_CATALOG=iceberg
```

O `.env` esta no `.gitignore`.

## Execucao

Subir o Trino:

```bash
docker compose up -d
```

Se `trino/catalog/iceberg.properties` for alterado, reinicie:

```bash
docker restart unb-trino
```

Testar conexao:

```bash
python scripts/trino/test_trino_connection.py
```

Explorar S3 e gerar perfil:

```bash
python scripts/trino/explore_siorg_s3.py
```

Criar tabelas Iceberg:

```bash
python scripts/trino/create_iceberg_tables.py
```

Carga completa recriando tabelas de dados:

```bash
python scripts/trino/load_s3_to_iceberg.py --mode recreate
```

Carga incremental, ignorando arquivos ja carregados:

```bash
python scripts/trino/load_s3_to_iceberg.py --mode append
```

Validar dados:

```bash
python scripts/trino/validate_iceberg_data.py
```

Benchmark simples:

```bash
python scripts/trino/benchmark_trino_queries.py --users 1
```

Concorrencia:

```bash
python scripts/trino/benchmark_trino_queries.py --users 5 --mode mixed-workload
python scripts/trino/benchmark_trino_queries.py --users 10 --mode mixed-workload
python scripts/trino/benchmark_trino_queries.py --users 20 --mode mixed-workload
python scripts/trino/benchmark_trino_queries.py --users 20 --mode same-query --same-query-id q01
```

## Resultados

Arquivos gerados em `results/trino/`:

- `s3_inventory.json`
- `s3_profile_report.md`
- `load_log.csv`
- `query_log.csv`
- `query_summary.csv`
- `validation_report.csv`

Tambem sao mantidas tabelas Iceberg:

- `iceberg.siorg.load_log`
- `iceberg.siorg.query_log`

## Decisoes metodologicas

- Os CSVs de origem em `iceberg/` sao ignorados; esse prefixo e reservado para metadados e dados Iceberg.
- `distribuicao/` possui dois schemas reais: arquivos de 36 colunas sem `quantidade` e arquivos de 37 colunas com `quantidade`. A tabela usa a uniao dos cabecalhos; arquivos antigos recebem `quantidade = NULL`.
- As colunas de origem sao `VARCHAR`. Os dados reais possuem campos mistos, URIs, textos longos, valores vazios, `nan` textual e colunas com conteudo multiline. Casts sao feitos nas consultas quando necessarios.
- Foram adicionadas colunas de auditoria: `source_file`, `reference_date`, `ano_referencia`, `mes_referencia`, `data_carga`.
- Nao ha particionamento inicial. As consultas agrupam por competencia, mas o volume academico e a carga via Trino favorecem uma primeira versao simples; particionamento pode ser avaliado depois com base em custo/beneficio medido.

## Consultas

As consultas Trino estao em `sql/trino/benchmark/` e preservam o objetivo analitico de `docs/Consultas.md`.

Adaptacoes principais:

- `lagInFrame` -> `lag`
- operador ternario ClickHouse -> `CASE WHEN`
- `toUInt8OrZero` -> `COALESCE(try_cast(... AS integer), 0)`

## Limitacoes conhecidas

- A carga usa processamento local para converter CSV em Parquet, grava os Parquets em `s3://unb-bdm-siorg/iceberg/staging/trino_iceberg/` e registra os arquivos com `ALTER TABLE ... EXECUTE add_files`. O catalogo precisa de `iceberg.add-files-procedure.enabled=true`.
- O benchmark busca o resultado completo para contar linhas retornadas, como no script ClickHouse existente. Consultas com muitos resultados podem consumir memoria do cliente.
- O script de validacao compara destino e metadados carregados; contagem exata da origem por arquivo depende da leitura completa durante a carga.

