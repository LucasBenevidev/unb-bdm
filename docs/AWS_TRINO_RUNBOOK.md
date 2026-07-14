# Runbook Trino AWS Fixed

Este runbook cria um ambiente temporario para rodar Trino/Iceberg em capacidade comparavel ao ClickHouse fixo do Lucas.

## 1. Provisionar EC2

```bash
python scripts/trino/aws_trino_env.py preflight
python scripts/trino/aws_trino_env.py provision
python scripts/trino/aws_trino_env.py status
python scripts/trino/aws_trino_env.py commands
```

Padrao:

- `m7i.2xlarge`
- `8 vCPU / 32 GiB`
- `80 GiB gp3`
- Ubuntu 22.04
- Security group com SSH liberado apenas para o IP publico atual

Se o `provision` falhar com:

```text
The specified instance type is not eligible for Free Tier
```

Lucas ainda precisa remover a restricao de conta/politica que limita `RunInstances`
a instancias Free Tier. O `m7i.2xlarge` e necessario para equivaler o ambiente
ClickHouse fixo em `8 vCPU / 32 GiB`.

## 2. Copiar o repositorio

Use o comando mostrado por:

```bash
python scripts/trino/aws_trino_env.py commands
```

Na EC2, entre no diretorio:

```bash
cd /opt/unb-bdm
```

## 3. Preparar Python e Trino

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
docker compose up -d
```

## 4. Rodar testes

```bash
python scripts/trino/test_trino_connection.py
python scripts/trino/create_iceberg_tables.py
python scripts/trino/load_s3_to_iceberg.py --mode recreate
python scripts/trino/validate_iceberg_data.py
python scripts/trino/benchmark_trino_queries.py --users 1 --skip-iceberg-log
python scripts/trino/benchmark_trino_queries.py --users 5 --skip-iceberg-log
python scripts/trino/benchmark_trino_queries.py --users 10 --skip-iceberg-log
python scripts/trino/benchmark_trino_queries.py --users 20 --skip-iceberg-log
```

## 5. Coletar armazenamento/compressao

Use o `id_execucao` completo de `results/trino/load_log.csv`.

```bash
mkdir -p results/trino_aws_fixed
cp results/trino/load_log.csv results/trino_aws_fixed/
cp results/trino/query_log.csv results/trino_aws_fixed/
cp results/trino/query_summary.csv results/trino_aws_fixed/
cp results/trino/validation_report.csv results/trino_aws_fixed/
python scripts/trino/collect_trino_storage.py --run-id <ID_COMPLETO> --output-dir results/trino_aws_fixed
```

## 6. Copiar resultados de volta

No computador local, use o comando `scp` mostrado por:

```bash
python scripts/trino/aws_trino_env.py commands
```

## 7. Encerrar ambiente

```bash
python scripts/trino/aws_trino_env.py terminate
```

Se a criacao falhar antes de existir instancia, limpe recursos parciais:

```bash
python scripts/trino/aws_trino_env.py cleanup
```

## 8. Gerar comparacao final

Depois que Lucas entregar `results/clickhouse_fixed/` e os resultados Trino
estiverem em `results/trino_aws_fixed/`:

```bash
python scripts/trino/compare_architectures.py --clickhouse-dir results/clickhouse_fixed --trino-dir results/trino_aws_fixed --output-dir results/comparison_fixed
```

