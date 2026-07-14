# unb-bdm

Repositório do trabalho de Bancos de Dados Massivos da UnB comparando duas arquiteturas para dados SIORG:

- **DW:** ClickHouse.
- **Lakehouse:** Trino + Apache Iceberg + AWS Glue Data Catalog + Amazon S3.

O objetivo do experimento é comparar ingestão, armazenamento/compressão e consultas analíticas.

## Estrutura

```text
.
├── docs/                  # Documentação do trabalho, consultas e runbooks
├── notebooks/             # Notebooks auxiliares de exploração e execução
├── scripts/
│   ├── clickhouse/        # Scripts da trilha ClickHouse
│   └── trino/             # Scripts da trilha Trino/Iceberg
├── sql/
│   └── trino/             # DDL, carga e consultas Trino
├── trino/
│   └── catalog/           # Configuração do catálogo Iceberg
├── results/               # Saídas de testes, ignoradas no Git
├── docker-compose.yaml    # Trino local em Docker
└── requirements.txt       # Dependências Python
```

## Configuração

Crie um arquivo `.env` na raiz. Ele é ignorado pelo Git.

```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=unb-bdm-siorg

TRINO_HOST=localhost
TRINO_PORT=8090
TRINO_USER=cassio
TRINO_CATALOG=iceberg
TRINO_SCHEMA=siorg
```

Instale as dependências:

```powershell
python -m pip install -r requirements.txt
```

## Trino/Iceberg

Subir o Trino local:

```powershell
docker compose up -d
```

Testar conexão:

```powershell
python scripts/trino/test_trino_connection.py
```

Explorar os arquivos SIORG no S3:

```powershell
python scripts/trino/explore_siorg_s3.py
```

Criar as tabelas Iceberg:

```powershell
python scripts/trino/create_iceberg_tables.py
```

Carregar todos os CSVs para Iceberg:

```powershell
python scripts/trino/load_s3_to_iceberg.py --mode recreate
```

Validar os dados carregados:

```powershell
python scripts/trino/validate_iceberg_data.py
```

Rodar benchmarks:

```powershell
python scripts/trino/benchmark_trino_queries.py --users 1
python scripts/trino/benchmark_trino_queries.py --users 5
python scripts/trino/benchmark_trino_queries.py --users 10
python scripts/trino/benchmark_trino_queries.py --users 20
```

Os resultados ficam em `results/trino/`.

## Execução Trino em AWS

Para rodar o Trino em uma EC2 fixa e evitar comparar com a máquina local:

```powershell
python scripts/trino/aws_trino_env.py preflight
python scripts/trino/aws_trino_env.py provision
python scripts/trino/aws_trino_env.py status
python scripts/trino/aws_trino_env.py commands
```

O runbook completo está em [docs/AWS_TRINO_RUNBOOK.md](docs/AWS_TRINO_RUNBOOK.md).

Depois dos testes, desligue a EC2:

```powershell
python scripts/trino/aws_trino_env.py terminate
```

## ClickHouse

Os scripts da trilha ClickHouse ficam em `scripts/clickhouse/`.

Carga:

```powershell
python scripts/clickhouse/load_s3_to_clickhouse.py
```

Benchmark:

```powershell
python scripts/clickhouse/benchmark_queries.py
```

Os workflows em `.github/workflows/` também apontam para esses caminhos.

## Comparação

Com resultados ClickHouse em `results/clickhouse` e Trino em `results/trino`:

```powershell
python scripts/trino/compare_architectures.py --clickhouse-dir results/clickhouse --trino-dir results/trino --output-dir results/comparison
```

Para a comparação final em ambiente controlado, use uma pasta separada para os novos resultados do ClickHouse, por exemplo:

```powershell
python scripts/trino/compare_architectures.py --clickhouse-dir results/clickhouse_fixed --trino-dir results/trino_aws_fixed --output-dir results/comparison_fixed
```

## Resultados

`results/` é ignorado pelo Git para evitar versionar saídas grandes. Os arquivos esperados são:

- `results/trino/load_log.csv`
- `results/trino/query_log.csv`
- `results/trino/query_summary.csv`
- `results/trino/validation_report.csv`
- `results/trino/storage_summary.csv`
- `results/comparison/comparison_report.md`
- `results/comparison/*.png`

## Documentação

- [docs/README_TRINO_ICEBERG.md](docs/README_TRINO_ICEBERG.md): detalhes da implementação Trino/Iceberg.
- [docs/AWS_TRINO_RUNBOOK.md](docs/AWS_TRINO_RUNBOOK.md): execução em EC2.
- [docs/Consultas.md](docs/Consultas.md): consultas originais da trilha ClickHouse.
- [docs/Lakehouse_vs_DW_UnB.pdf](docs/Lakehouse_vs_DW_UnB.pdf): material do trabalho.
