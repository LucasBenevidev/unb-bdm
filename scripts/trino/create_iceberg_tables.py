from __future__ import annotations

import argparse
import sys

from trino.exceptions import TrinoUserError

from siorg_trino_common import TABLES, get_bucket_name, get_trino_connection, qualified_table, quote_identifier, setup_logging


logger = setup_logging("create_iceberg_tables")


def execute(cur, sql: str) -> None:
    logger.info("Executando SQL: %s", " ".join(sql.split())[:240])
    cur.execute(sql)


def data_table_ddl(table_name: str, columns: list[str]) -> str:
    bucket = get_bucket_name()
    csv_columns = ",\n    ".join(f"{quote_identifier(column)} VARCHAR" for column in columns)
    return f"""
CREATE TABLE IF NOT EXISTS {qualified_table(table_name)} (
    {csv_columns},
    ano_referencia INTEGER,
    mes_referencia INTEGER,
    reference_date DATE,
    source_file VARCHAR,
    data_carga TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    location = 's3://{bucket}/iceberg/siorg/{table_name}/'
)
"""


def load_log_ddl() -> str:
    bucket = get_bucket_name()
    return f"""
CREATE TABLE IF NOT EXISTS {qualified_table("load_log")} (
    id_execucao INTEGER,
    data_execucao TIMESTAMP(6),
    engine VARCHAR,
    arquivo VARCHAR,
    tabela VARCHAR,
    duracao_segundos DOUBLE,
    total_linhas BIGINT,
    linhas_por_segundo DOUBLE,
    bytes_origem BIGINT,
    status VARCHAR,
    erro VARCHAR
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    location = 's3://{bucket}/iceberg/siorg/load_log/'
)
"""


def query_log_ddl() -> str:
    bucket = get_bucket_name()
    return f"""
CREATE TABLE IF NOT EXISTS {qualified_table("query_log")} (
    id_execucao INTEGER,
    data_execucao TIMESTAMP(6),
    engine VARCHAR,
    query_id VARCHAR,
    query_name VARCHAR,
    trino_query_id VARCHAR,
    duracao_segundos DOUBLE,
    linhas_retornadas BIGINT,
    usuarios INTEGER,
    modo VARCHAR,
    status VARCHAR,
    erro VARCHAR
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    location = 's3://{bucket}/iceberg/siorg/query_log/'
)
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Cria tabelas Iceberg SIORG no catalogo Trino.")
    parser.add_argument("--drop-data", action="store_true", help="Remove e recria apenas as tabelas de dados.")
    parser.add_argument("--drop-logs", action="store_true", help="Remove e recria tabelas de logs.")
    args = parser.parse_args()

    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        bucket = get_bucket_name()
        execute(cur, f"CREATE SCHEMA IF NOT EXISTS iceberg.siorg WITH (location = 's3://{bucket}/iceberg/siorg/')")

        if args.drop_data:
            for table_name in TABLES:
                execute(cur, f"DROP TABLE IF EXISTS {qualified_table(table_name)}")
        if args.drop_logs:
            execute(cur, f"DROP TABLE IF EXISTS {qualified_table('load_log')}")
            execute(cur, f"DROP TABLE IF EXISTS {qualified_table('query_log')}")

        for table_name, config in TABLES.items():
            execute(cur, data_table_ddl(table_name, config["columns"]))
            logger.info("Tabela verificada/criada: %s", table_name)
        execute(cur, load_log_ddl())
        execute(cur, query_log_ddl())
        logger.info("Tabelas de log verificadas/criadas.")
        conn.close()
        return 0
    except TrinoUserError as exc:
        logger.error("Erro do Trino ao criar tabelas: %s", exc)
        return 1
    except OSError as exc:
        logger.error("Falha de conexao com Trino: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
