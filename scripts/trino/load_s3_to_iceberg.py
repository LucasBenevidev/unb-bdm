from __future__ import annotations

import argparse
import csv
import io
import tempfile
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError
from trino.exceptions import TrinoUserError

from create_iceberg_tables import data_table_ddl
from siorg_trino_common import (
    ENGINE,
    TABLES,
    ensure_results_dir,
    extract_reference_date,
    get_bucket_name,
    get_s3_client,
    get_trino_connection,
    list_source_csv_objects,
    qualified_table,
    quote_identifier,
    setup_logging,
    table_for_key,
    write_csv_rows,
)


logger = setup_logging("load_s3_to_iceberg", "siorg_trino_load.log")

LOAD_LOG_FIELDS = [
    "id_execucao",
    "data_execucao",
    "engine",
    "arquivo",
    "tabela",
    "duracao_segundos",
    "total_linhas",
    "linhas_por_segundo",
    "bytes_origem",
    "status",
    "erro",
]


def next_execution_id(cur) -> int:
    try:
        cur.execute(f"SELECT COALESCE(max(id_execucao), 0) + 1 FROM {qualified_table('load_log')}")
        return int(cur.fetchone()[0])
    except TrinoUserError:
        return 1


def recreate_data_tables(cur) -> None:
    for table_name, config in TABLES.items():
        cur.execute(f"DROP TABLE IF EXISTS {qualified_table(table_name)}")
        cur.execute(data_table_ddl(table_name, config["columns"]))


def insert_load_log(cur, row: dict[str, Any]) -> None:
    columns = LOAD_LOG_FIELDS
    quoted = ", ".join(quote_identifier(column) for column in columns)
    casts = [
        "CAST(? AS INTEGER)",
        "CAST(? AS TIMESTAMP(6))",
        "CAST(? AS VARCHAR)",
        "CAST(? AS VARCHAR)",
        "CAST(? AS VARCHAR)",
        "CAST(? AS DOUBLE)",
        "CAST(? AS BIGINT)",
        "CAST(? AS DOUBLE)",
        "CAST(? AS BIGINT)",
        "CAST(? AS VARCHAR)",
        "CAST(? AS VARCHAR)",
    ]
    sql = f"INSERT INTO {qualified_table('load_log')} ({quoted}) SELECT {', '.join(casts)}"
    cur.execute(sql, [row[column] for column in columns])


def source_file_already_loaded(cur, table_name: str, key: str) -> bool:
    cur.execute(
        f"SELECT count(*) FROM {qualified_table(table_name)} WHERE source_file = ?",
        [key],
    )
    return int(cur.fetchone()[0]) > 0


def arrow_schema(columns: list[str]) -> pa.Schema:
    return pa.schema(
        [(column, pa.string()) for column in columns]
        + [
            ("ano_referencia", pa.int32()),
            ("mes_referencia", pa.int32()),
            ("reference_date", pa.date32()),
            ("source_file", pa.string()),
            ("data_carga", pa.timestamp("us")),
        ]
    )


def convert_csv_to_parquet(
    s3_client: Any,
    bucket: str,
    key: str,
    table_name: str,
    local_path: Path,
    chunksize: int,
) -> int:
    reference_date = extract_reference_date(key)
    if reference_date is None:
        raise ValueError(f"Nao foi possivel extrair competencia YYYY-MM de {key}")

    columns = TABLES[table_name]["columns"]
    schema = arrow_schema(columns)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    text_stream = io.TextIOWrapper(body, encoding="utf-8-sig", newline="")
    data_carga = datetime.now()
    total_rows = 0

    writer: pq.ParquetWriter | None = None
    try:
        for chunk in pd.read_csv(
            text_stream,
            dtype="string",
            chunksize=chunksize,
            keep_default_na=True,
            na_values=["", "nan", "NaN", "NULL", "null"],
        ):
            for column in columns:
                if column not in chunk.columns:
                    chunk[column] = pd.NA
            chunk = chunk[columns]
            chunk["ano_referencia"] = reference_date.year
            chunk["mes_referencia"] = reference_date.month
            chunk["reference_date"] = pd.to_datetime(reference_date)
            chunk["source_file"] = key
            chunk["data_carga"] = data_carga
            table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(local_path, schema=schema, compression="zstd")
            writer.write_table(table)
            total_rows += len(chunk)
    finally:
        if writer is not None:
            writer.close()
    return total_rows


def upload_parquet_to_s3(
    s3_client: Any,
    bucket: str,
    local_path: Path,
    id_execucao: int,
    table_name: str,
    key: str,
) -> str:
    safe_name = Path(key).name.replace(".csv", ".parquet")
    source_stem = Path(safe_name).stem
    s3_key = f"iceberg/staging/trino_iceberg/run_{id_execucao}/{table_name}/{source_stem}/{safe_name}"
    s3_client.upload_file(str(local_path), bucket, s3_key)
    return f"s3://{bucket}/{s3_key}"


def add_parquet_file_to_iceberg(cur, table_name: str, parquet_uri: str) -> None:
    location = parquet_uri.rsplit("/", 1)[0] + "/"
    sql = (
        f"ALTER TABLE {qualified_table(table_name)} "
        "EXECUTE add_files("
        f"location => '{location}', "
        "format => 'PARQUET')"
    )
    cur.execute(sql)


def load_one_file(
    s3_client: Any,
    bucket: str,
    cur,
    key: str,
    size: int,
    table_name: str,
    chunksize: int,
    id_execucao: int,
) -> dict[str, Any]:
    start = time.perf_counter()
    data_execucao = datetime.now()
    total_rows = 0
    status = "success"
    error = ""
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / (Path(key).name.replace(".csv", ".parquet"))
            total_rows = convert_csv_to_parquet(
                s3_client=s3_client,
                bucket=bucket,
                key=key,
                table_name=table_name,
                local_path=local_path,
                chunksize=chunksize,
            )
            parquet_uri = upload_parquet_to_s3(
                s3_client=s3_client,
                bucket=bucket,
                local_path=local_path,
                id_execucao=id_execucao,
                table_name=table_name,
                key=key,
            )
            add_parquet_file_to_iceberg(cur, table_name, parquet_uri)
    except (
        BotoCoreError,
        ClientError,
        TrinoUserError,
        csv.Error,
        UnicodeError,
        OSError,
        ValueError,
        pd.errors.ParserError,
        pa.ArrowException,
    ) as exc:
        status = "error"
        error = str(exc)
        logger.error("Falha ao carregar %s: %s", key, exc)

    duration = time.perf_counter() - start
    return {
        "id_execucao": id_execucao,
        "data_execucao": data_execucao.isoformat(sep=" ", timespec="seconds"),
        "engine": ENGINE,
        "arquivo": key,
        "tabela": table_name,
        "duracao_segundos": round(duration, 6),
        "total_linhas": total_rows,
        "linhas_por_segundo": round(total_rows / duration, 6) if duration > 0 else 0,
        "bytes_origem": size,
        "status": status,
        "erro": error,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Carrega CSVs SIORG do S3 para tabelas Iceberg via Trino.")
    parser.add_argument("--mode", choices=["recreate", "append"], default="append")
    parser.add_argument("--batch-size", type=int, default=50000, help="Linhas por chunk na conversao CSV->Parquet.")
    parser.add_argument("--limit-files", type=int, default=None, help="Uso para smoke test.")
    args = parser.parse_args()

    try:
        s3_client = get_s3_client()
        bucket = get_bucket_name()
        conn = get_trino_connection()
        cur = conn.cursor()

        if args.mode == "recreate":
            logger.info("Modo recreate: removendo e recriando tabelas de dados Iceberg.")
            recreate_data_tables(cur)

        id_execucao = next_execution_id(cur)
        logger.info("id_execucao atribuido: %s", id_execucao)

        objects = [
            item
            for item in list_source_csv_objects(s3_client, bucket)
            if table_for_key(item.key) is not None
        ]
        if args.limit_files:
            objects = objects[: args.limit_files]

        csv_log_path = ensure_results_dir() / "load_log.csv"
        for item in objects:
            table_name = table_for_key(item.key)
            if table_name is None:
                continue
            if args.mode == "append" and source_file_already_loaded(cur, table_name, item.key):
                logger.info("Arquivo ja carregado, ignorando: %s", item.key)
                continue
            logger.info("Carregando %s em %s", item.key, table_name)
            log_row = load_one_file(
                s3_client=s3_client,
                bucket=bucket,
                cur=cur,
                key=item.key,
                size=item.size,
                table_name=table_name,
                chunksize=args.batch_size,
                id_execucao=id_execucao,
            )
            write_csv_rows(csv_log_path, LOAD_LOG_FIELDS, [log_row])
            try:
                insert_load_log(cur, log_row)
            except TrinoUserError as exc:
                logger.error("Falha ao registrar log Iceberg para %s: %s", item.key, exc)
            logger.info(
                "%s: %s linhas em %.2fs (%s)",
                log_row["status"].upper(),
                log_row["total_linhas"],
                log_row["duracao_segundos"],
                item.key,
            )
        conn.close()
        return 0
    except (BotoCoreError, ClientError, TrinoUserError, OSError) as exc:
        logger.error("Falha na carga: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
