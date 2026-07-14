from __future__ import annotations

import argparse
import csv
import os
import tempfile
from pathlib import Path
from typing import Any

import boto3
import pyarrow.parquet as pq
from dotenv import load_dotenv

from siorg_trino_common import TABLES, ensure_results_dir, get_bucket_name, setup_logging


logger = setup_logging("collect_trino_storage")


def list_objects(bucket: str, prefix: str) -> list[dict[str, Any]]:
    s3 = boto3.client("s3")
    objects: list[dict[str, Any]] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return [obj for obj in objects if obj["Key"].endswith(".parquet")]


def source_csv_bytes(bucket: str, table_name: str) -> int:
    prefix = TABLES[table_name]["prefix"]
    s3 = boto3.client("s3")
    total = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".csv"):
                total += int(obj["Size"])
    return total


def parquet_uncompressed_bytes(bucket: str, key: str) -> int:
    s3 = boto3.client("s3")
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        s3.download_file(bucket, key, str(tmp_path))
        metadata = pq.ParquetFile(tmp_path).metadata
        total = 0
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                total += row_group.column(column_index).total_uncompressed_size
        return total
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass


def collect_for_run(bucket: str, run_id: int) -> list[dict[str, Any]]:
    rows = []
    for table_name in ["distribuicao_orgaos", "estrutura_organizacional_completa"]:
        prefix = f"iceberg/staging/trino_iceberg/run_{run_id}/{table_name}/"
        objects = list_objects(bucket, prefix)
        compressed = sum(int(obj["Size"]) for obj in objects)
        uncompressed = 0
        for index, obj in enumerate(objects, 1):
            logger.info("Lendo metadados Parquet %s/%s: %s", index, len(objects), obj["Key"])
            uncompressed += parquet_uncompressed_bytes(bucket, obj["Key"])
        csv_bytes = source_csv_bytes(bucket, table_name)
        rows.append(
            {
                "engine": "trino_iceberg",
                "run_id": run_id,
                "table": table_name,
                "parquet_files": len(objects),
                "compressed_bytes": compressed,
                "compressed_mib": round(compressed / 1024 / 1024, 6),
                "uncompressed_bytes": uncompressed,
                "uncompressed_mib": round(uncompressed / 1024 / 1024, 6),
                "compression_ratio": round(uncompressed / compressed, 6) if compressed else 0,
                "source_csv_bytes": csv_bytes,
                "csv_to_parquet_ratio": round(csv_bytes / compressed, 6) if compressed else 0,
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Coleta armazenamento e compressao dos Parquets Trino/Iceberg.")
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    load_dotenv(".env")
    bucket = get_bucket_name()
    rows = collect_for_run(bucket, args.run_id)
    out_dir = Path(args.output_dir) if args.output_dir else ensure_results_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "storage_summary.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Resumo de armazenamento gravado em %s", path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

