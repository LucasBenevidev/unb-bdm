from __future__ import annotations

import csv
import io
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import boto3
from botocore.client import BaseClient
from dotenv import load_dotenv
from trino.dbapi import Connection, connect


ENGINE = "trino_iceberg"
DEFAULT_CATALOG = "iceberg"
DEFAULT_SCHEMA = "siorg"
DEFAULT_TRINO_HOST = "localhost"
DEFAULT_TRINO_PORT = 8090
DEFAULT_TRINO_USER = "cassio"
DEFAULT_BUCKET = "unb-bdm-siorg"
DEFAULT_REGION = "us-east-1"
RESULTS_DIR = Path("results") / "trino"


DISTRIBUICAO_COLUMNS = [
    "nivel_hierarquico",
    "cod_orgao_entidade",
    "nome_orgao_entidade",
    "sigla_orgao_entidade",
    "natureza_juridica",
    "subnatureza_juridica",
    "poder",
    "esfera",
    "cod_unidade_pai",
    "cod_unidade",
    "nome_unidade",
    "sigla_unidade",
    "endereco",
    "endereco_complemento",
    "bairro",
    "municipio",
    "uf",
    "cep",
    "telefone",
    "email",
    "area_atuacao",
    "nivel_normatizacao",
    "autonomia_gestao_cargos",
    "tipo_cargo",
    "categoria_cargo",
    "nivel_cargo",
    "quantidade",
    "denominacao_cargo",
    "complemento_denominacao",
    "mobilidade",
    "obriga_distribuicao",
    "compoe_estrutura",
    "autoridade",
    "regra_autoridade",
    "regra_cargo_nome_unidade",
    "temporario",
    "cod_instancia",
]

COMPLETA_COLUMNS = [
    "codigoUnidade",
    "codigoUnidadePai",
    "codigoOrgaoEntidade",
    "codigoTipoUnidade",
    "nome",
    "sigla",
    "codigoEsfera",
    "codigoPoder",
    "codigoNaturezaJuridica",
    "codigoSubNaturezaJuridica",
    "nivelNormatizacao",
    "versaoConsulta",
    "dataInicialVersaoConsulta",
    "dataFinalVersaoConsulta",
    "operacao",
    "codigoUnidadePaiAnterior",
    "codigoOrgaoEntidadeAnterior",
    "regulamentoEspecifico",
    "codigoCategoriaUnidade",
    "competencia",
    "finalidade",
    "missao",
    "descricaoAtoNormativo",
    "areaAtuacao",
    "telefone",
    "email",
    "site",
    "linhaEndereco",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "pais",
    "horarioFuncionamento",
]

TABLES: dict[str, dict[str, Any]] = {
    "distribuicao_orgaos": {
        "prefix": "distribuicao/",
        "columns": DISTRIBUICAO_COLUMNS,
    },
    "estrutura_organizacional_completa": {
        "prefix": "estrutura-organizacional-completa/",
        "columns": COMPLETA_COLUMNS,
    },
}


@dataclass(frozen=True)
class S3Object:
    key: str
    size: int
    last_modified: datetime | None = None


def setup_logging(name: str, log_file: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    if log_file:
        log_path = Path(log_file)
        if not log_path.is_absolute():
            log_path = Path("logs") / log_path
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


def load_environment() -> None:
    load_dotenv(".env")


def get_bucket_name() -> str:
    return os.getenv("S3_BUCKET_NAME", DEFAULT_BUCKET)


def get_region() -> str:
    return os.getenv("AWS_DEFAULT_REGION", DEFAULT_REGION)


def get_s3_client() -> BaseClient:
    load_environment()
    return boto3.client("s3", region_name=get_region())


def get_trino_connection(schema: str = DEFAULT_SCHEMA) -> Connection:
    load_environment()
    return connect(
        host=os.getenv("TRINO_HOST", DEFAULT_TRINO_HOST),
        port=int(os.getenv("TRINO_PORT", str(DEFAULT_TRINO_PORT))),
        user=os.getenv("TRINO_USER", DEFAULT_TRINO_USER),
        catalog=os.getenv("TRINO_CATALOG", DEFAULT_CATALOG),
        schema=schema,
        request_timeout=float(os.getenv("TRINO_REQUEST_TIMEOUT", "300")),
    )


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qualified_table(table: str) -> str:
    return f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{quote_identifier(table)}"


def ensure_results_dir() -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    return RESULTS_DIR


def list_source_csv_objects(s3_client: BaseClient, bucket: str) -> list[S3Object]:
    objects: list[S3Object] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".csv") and not key.startswith("iceberg/"):
                objects.append(
                    S3Object(
                        key=key,
                        size=int(obj["Size"]),
                        last_modified=obj.get("LastModified"),
                    )
                )
    return sorted(objects, key=lambda item: item.key)


def table_for_key(key: str) -> str | None:
    for table, config in TABLES.items():
        if key.startswith(config["prefix"]):
            return table
    return None


def extract_reference_date(key: str) -> date | None:
    match = re.search(r"(\d{4})-(\d{2})", key)
    if not match:
        return None
    return date(int(match.group(1)), int(match.group(2)), 1)


def read_s3_range_text(
    s3_client: BaseClient, bucket: str, key: str, byte_count: int = 1024 * 1024
) -> tuple[str, str]:
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
        Range=f"bytes=0-{byte_count - 1}",
    )
    payload = response["Body"].read()
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return payload.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return payload.decode("latin-1", errors="replace"), "latin-1-replace"


def detect_csv_dialect(sample_text: str) -> csv.Dialect:
    return csv.Sniffer().sniff(sample_text[:10000], delimiters=",;|\t")


def read_csv_header(s3_client: BaseClient, bucket: str, key: str) -> tuple[list[str], str, str]:
    text, encoding = read_s3_range_text(s3_client, bucket, key, 128 * 1024)
    dialect = detect_csv_dialect(text)
    reader = csv.reader(io.StringIO(text), dialect)
    header = next(reader)
    return [column.strip().replace("\ufeff", "") for column in header], encoding, dialect.delimiter


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_csv_rows(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
