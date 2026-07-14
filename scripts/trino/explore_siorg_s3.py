from __future__ import annotations

import argparse
import csv
import io
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from siorg_trino_common import (
    TABLES,
    detect_csv_dialect,
    ensure_results_dir,
    extract_reference_date,
    get_bucket_name,
    get_s3_client,
    list_source_csv_objects,
    read_s3_range_text,
    setup_logging,
)


logger = setup_logging("explore_siorg_s3", "siorg_trino_explore.log")


def classify_value(value: str | None) -> str:
    text = (value or "").strip()
    if text == "" or text.lower() in {"nan", "null", "none"}:
        return "null"
    if re.fullmatch(r"[+-]?\d+", text):
        return "integer"
    if re.fullmatch(r"[+-]?\d+[\.,]\d+", text):
        return "decimal"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return "date"
    if text.startswith(("http://", "https://")):
        return "url"
    return "text"


def summarize_prefix(prefix: str, items: list[Any]) -> dict[str, Any]:
    sizes = [item.size for item in items]
    months = [extract_reference_date(item.key) for item in items]
    months = [item for item in months if item is not None]
    return {
        "prefix": prefix,
        "quantidade_arquivos": len(items),
        "tamanho_total_mb": round(sum(sizes) / 1024 / 1024, 2),
        "tamanho_medio_mb": round(mean(sizes) / 1024 / 1024, 2),
        "menor_arquivo_mb": round(min(sizes) / 1024 / 1024, 2),
        "maior_arquivo_mb": round(max(sizes) / 1024 / 1024, 2),
        "periodo_inicial": min(months).isoformat() if months else None,
        "periodo_final": max(months).isoformat() if months else None,
    }


def profile_group(s3_client: Any, bucket: str, prefix: str, items: list[Any], sample_rows: int) -> dict[str, Any]:
    header_counts: Counter[tuple[str, ...]] = Counter()
    header_examples: dict[tuple[str, ...], str] = {}
    encoding_counts: Counter[str] = Counter()
    delimiter_counts: Counter[str] = Counter()

    for item in items:
        text, encoding = read_s3_range_text(s3_client, bucket, item.key, 256 * 1024)
        dialect = detect_csv_dialect(text)
        header = tuple(next(csv.reader(io.StringIO(text), dialect)))
        header = tuple(column.strip().replace("\ufeff", "") for column in header)
        header_counts[header] += 1
        header_examples.setdefault(header, item.key)
        encoding_counts[encoding] += 1
        delimiter_counts[dialect.delimiter] += 1

    sample_indexes = sorted({0, len(items) // 2, len(items) - 1})
    sampled_files = [items[index].key for index in sample_indexes]
    columns = sorted({column for header in header_counts for column in header})
    type_counts: dict[str, Counter[str]] = {column: Counter() for column in columns}
    null_counts: Counter[str] = Counter()
    seen_counts: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)

    for key in sampled_files:
        text, _ = read_s3_range_text(s3_client, bucket, key, 8 * 1024 * 1024)
        dialect = detect_csv_dialect(text)
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        for row_index, row in enumerate(reader):
            if row_index >= sample_rows:
                break
            for column in columns:
                value = row.get(column)
                value_type = classify_value(value)
                type_counts[column][value_type] += 1
                seen_counts[column] += 1
                if value_type == "null":
                    null_counts[column] += 1
                elif len(examples[column]) < 3 and value not in examples[column]:
                    examples[column].append((value or "")[:120])

    return {
        "prefix": prefix,
        "encodings": dict(encoding_counts),
        "delimiters": dict(delimiter_counts),
        "distinct_headers": len(header_counts),
        "headers": [
            {
                "quantidade_arquivos": count,
                "arquivo_exemplo": header_examples[header],
                "quantidade_colunas": len(header),
                "colunas": list(header),
            }
            for header, count in header_counts.items()
        ],
        "sampled_files": sampled_files,
        "columns": [
            {
                "coluna": column,
                "tipos_amostra": dict(type_counts[column]),
                "percentual_nulos_amostra": round(
                    (null_counts[column] / seen_counts[column] * 100) if seen_counts[column] else 0,
                    2,
                ),
                "exemplos": examples[column],
            }
            for column in columns
        ],
    }


def write_markdown_report(path: Path, inventory: list[dict[str, Any]], profiles: list[dict[str, Any]]) -> None:
    lines = ["# Exploracao SIORG S3", ""]
    lines.append("## Inventario")
    lines.append("")
    lines.append("| prefixo | arquivos | total MB | medio MB | menor MB | maior MB | periodo |")
    lines.append("|---|---:|---:|---:|---:|---:|---|")
    for item in inventory:
        period = f"{item['periodo_inicial']} a {item['periodo_final']}"
        lines.append(
            f"| {item['prefix']} | {item['quantidade_arquivos']} | {item['tamanho_total_mb']} | "
            f"{item['tamanho_medio_mb']} | {item['menor_arquivo_mb']} | {item['maior_arquivo_mb']} | {period} |"
        )
    for profile in profiles:
        lines.extend(["", f"## Prefixo `{profile['prefix']}`", ""])
        lines.append(f"Encodings: `{profile['encodings']}`. Delimitadores: `{profile['delimiters']}`.")
        lines.append(f"Cabecalhos distintos: {profile['distinct_headers']}.")
        for header in profile["headers"]:
            lines.append(
                f"- {header['quantidade_arquivos']} arquivo(s), {header['quantidade_colunas']} coluna(s), "
                f"exemplo `{header['arquivo_exemplo']}`: {', '.join(header['colunas'])}"
            )
        lines.extend(["", "| coluna | tipos na amostra | % nulos | exemplos |", "|---|---|---:|---|"])
        for column in profile["columns"]:
            lines.append(
                f"| {column['coluna']} | `{column['tipos_amostra']}` | "
                f"{column['percentual_nulos_amostra']} | {', '.join(column['exemplos'])} |"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Inventaria e perfila arquivos CSV SIORG no S3.")
    parser.add_argument("--sample-rows", type=int, default=5000)
    args = parser.parse_args()

    try:
        s3_client = get_s3_client()
        bucket = get_bucket_name()
        objects = list_source_csv_objects(s3_client, bucket)
    except (BotoCoreError, ClientError) as exc:
        logger.error("Falha ao acessar o S3: %s", exc)
        return 1

    grouped: dict[str, list[Any]] = defaultdict(list)
    for item in objects:
        prefix = item.key.split("/")[0]
        if any(item.key.startswith(config["prefix"]) for config in TABLES.values()):
            grouped[prefix].append(item)

    inventory = [summarize_prefix(prefix, items) for prefix, items in sorted(grouped.items())]
    profiles = [
        profile_group(s3_client, bucket, prefix, items, args.sample_rows)
        for prefix, items in sorted(grouped.items())
    ]

    output_dir = ensure_results_dir()
    (output_dir / "s3_inventory.json").write_text(
        json.dumps({"bucket": bucket, "inventory": inventory, "profiles": profiles}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_markdown_report(output_dir / "s3_profile_report.md", inventory, profiles)

    for item in inventory:
        logger.info(
            "%s: %s arquivos, %.2f MB, periodo %s a %s",
            item["prefix"],
            item["quantidade_arquivos"],
            item["tamanho_total_mb"],
            item["periodo_inicial"],
            item["periodo_final"],
        )
    logger.info("Relatorios gravados em %s", output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

