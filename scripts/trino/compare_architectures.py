from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from siorg_trino_common import get_bucket_name, get_s3_client, setup_logging


logger = setup_logging("compare_architectures")

CLICKHOUSE_DIR = Path("results") / "clickhouse"
TRINO_DIR = Path("results") / "trino"
COMPARE_DIR = Path("results") / "comparison"


def percentile(series: pd.Series, q: float) -> float:
    if series.empty:
        return 0.0
    return float(series.quantile(q))


def summarize_ingestion(clickhouse_dir: Path, trino_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    ch_path = clickhouse_dir / "log_carga.csv"
    if ch_path.exists():
        ch = pd.read_csv(ch_path)
        complete = ch.groupby("id_execucao").filter(lambda group: len(group) == 91)
        per_run = (
            complete.groupby("id_execucao")
            .agg(
                arquivos=("nome_arquivo", "count"),
                total_linhas=("total_linhas", "sum"),
                duracao_segundos=("tempo_carga", "sum"),
            )
            .reset_index()
        )
        per_run["linhas_por_segundo"] = per_run["total_linhas"] / per_run["duracao_segundos"]
        for _, row in per_run.iterrows():
            rows.append(
                {
                    "engine": clickhouse_dir.name,
                    "id_execucao": int(row["id_execucao"]),
                    "arquivos": int(row["arquivos"]),
                    "total_linhas": int(row["total_linhas"]),
                    "duracao_segundos": float(row["duracao_segundos"]),
                    "linhas_por_segundo": float(row["linhas_por_segundo"]),
                }
            )

    trino_path = trino_dir / "load_log.csv"
    if trino_path.exists():
        trino = pd.read_csv(trino_path)
        if "status" in trino.columns:
            trino = trino[trino["status"].str.lower() == "success"]
        complete = trino.groupby("id_execucao").filter(lambda group: len(group) == 91)
        per_run = (
            complete.groupby("id_execucao")
            .agg(
                arquivos=("arquivo", "count"),
                total_linhas=("total_linhas", "sum"),
                duracao_segundos=("duracao_segundos", "sum"),
            )
            .reset_index()
        )
        per_run["linhas_por_segundo"] = per_run["total_linhas"] / per_run["duracao_segundos"]
        for _, row in per_run.iterrows():
            rows.append(
                {
                    "engine": trino_dir.name,
                    "id_execucao": int(row["id_execucao"]),
                    "arquivos": int(row["arquivos"]),
                    "total_linhas": int(row["total_linhas"]),
                    "duracao_segundos": float(row["duracao_segundos"]),
                    "linhas_por_segundo": float(row["linhas_por_segundo"]),
                }
            )

    return pd.DataFrame(rows)


def normalize_query_id(name: str) -> str:
    if "Query 1" in name or "Expansao" in name or "Expansão" in name:
        return "q01"
    if "Query 2" in name or "Relocaliz" in name:
        return "q02"
    if "Query 3" in name or "Complexidade" in name:
        return "q03"
    return name


def summarize_queries(clickhouse_dir: Path, trino_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    ch_path = clickhouse_dir / "log_consultas.csv"
    if ch_path.exists():
        ch = pd.read_csv(ch_path)
        ch["query_id_norm"] = ch["query_name"].map(normalize_query_id)
        ch = ch[ch["result_rows"] > 0]
        for query_id, group in ch.groupby("query_id_norm"):
            durations = group["elapsed_seconds"].astype(float)
            rows.append(
                {
                    "engine": clickhouse_dir.name,
                    "usuarios": 1,
                    "modo": "sequential",
                    "query_id": query_id,
                    "query_name": group["query_name"].iloc[-1],
                    "count": len(group),
                    "mean": durations.mean(),
                    "min": durations.min(),
                    "max": durations.max(),
                    "p50": percentile(durations, 0.50),
                    "p95": percentile(durations, 0.95),
                    "p99": percentile(durations, 0.99),
                    "result_rows_median": group["result_rows"].median(),
                    "read_rows_median": group["read_rows"].median(),
                    "read_bytes_median": group["read_bytes"].median(),
                }
            )

    trino_path = trino_dir / "query_log.csv"
    if trino_path.exists():
        trino = pd.read_csv(trino_path)
        if "status" in trino.columns:
            trino = trino[trino["status"].str.lower() == "success"]
        trino = trino[trino["linhas_retornadas"] > 0]
        expected_rows = {"q01": 46, "q02": 9849, "q03": 13927}
        trino = trino[
            trino.apply(
                lambda row: int(row["linhas_retornadas"]) == expected_rows.get(row["query_id"], int(row["linhas_retornadas"])),
                axis=1,
            )
        ]
        for keys, group in trino.groupby(["usuarios", "modo", "query_id"]):
            users, mode, query_id = keys
            durations = group["duracao_segundos"].astype(float)
            rows.append(
                {
                    "engine": trino_dir.name,
                    "usuarios": int(users),
                    "modo": mode,
                    "query_id": query_id,
                    "query_name": group["query_name"].iloc[-1],
                    "count": len(group),
                    "mean": durations.mean(),
                    "min": durations.min(),
                    "max": durations.max(),
                    "p50": percentile(durations, 0.50),
                    "p95": percentile(durations, 0.95),
                    "p99": percentile(durations, 0.99),
                    "result_rows_median": group["linhas_retornadas"].median(),
                    "read_rows_median": None,
                    "read_bytes_median": None,
                }
            )

    return pd.DataFrame(rows)


def collect_trino_storage(trino_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    summary_path = trino_dir / "storage_summary.csv"
    if summary_path.exists():
        storage = pd.read_csv(summary_path)
        if "engine" not in storage.columns:
            storage.insert(0, "engine", trino_dir.name)
        else:
            storage["engine"] = trino_dir.name
        return storage
    trino_path = trino_dir / "load_log.csv"
    if not trino_path.exists():
        return pd.DataFrame(rows)
    try:
        logs = pd.read_csv(trino_path)
        if "status" in logs.columns:
            logs = logs[logs["status"].str.lower() == "success"]
        complete_ids = logs.groupby("id_execucao").filter(lambda group: len(group) == 91)["id_execucao"]
        if complete_ids.empty:
            return pd.DataFrame(rows)
        latest_id = int(complete_ids.max())
        s3 = get_s3_client()
        bucket = get_bucket_name()
        paginator = s3.get_paginator("list_objects_v2")
        for table in ["distribuicao_orgaos", "estrutura_organizacional_completa"]:
            prefix = f"iceberg/staging/trino_iceberg/run_{latest_id}/{table}/"
            total = 0
            files = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    total += int(obj["Size"])
                    files += 1
            rows.append(
                {
                    "engine": "trino_iceberg",
                    "tabela": table,
                    "arquivos_dados": files,
                    "bytes_dados": total,
                    "mb_dados": total / 1024 / 1024,
                }
            )
    except Exception as exc:
        logger.error("Fallback S3 para armazenamento Trino tambem falhou: %s", exc)
    return pd.DataFrame(rows)


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    rendered = df.copy()
    for column in rendered.columns:
        if pd.api.types.is_float_dtype(rendered[column]):
            rendered[column] = rendered[column].map(lambda value: "" if pd.isna(value) else f"{value:.6f}")
        else:
            rendered[column] = rendered[column].map(lambda value: "" if pd.isna(value) else str(value))
    headers = list(rendered.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in rendered.iterrows():
        lines.append("| " + " | ".join(str(row[column]).replace("\n", " ") for column in headers) + " |")
    return "\n".join(lines)


def write_markdown(ingestion: pd.DataFrame, queries: pd.DataFrame, storage: pd.DataFrame, path: Path) -> None:
    lines = ["# Comparacao ClickHouse vs Trino/Iceberg", ""]
    lines.append("## Ingestao")
    lines.append("")
    if ingestion.empty:
        lines.append("Sem execucoes completas comparaveis de ingestao.")
    else:
        summary = (
            ingestion.groupby("engine")
            .agg(
                execucoes=("id_execucao", "count"),
                duracao_media_s=("duracao_segundos", "mean"),
                duracao_min_s=("duracao_segundos", "min"),
                duracao_max_s=("duracao_segundos", "max"),
                throughput_medio_lps=("linhas_por_segundo", "mean"),
                linhas=("total_linhas", "median"),
            )
            .reset_index()
        )
        lines.append(dataframe_to_markdown(summary))
    lines.extend(["", "## Consultas", ""])
    if queries.empty:
        lines.append("Sem logs de consulta comparaveis.")
    else:
        cols = ["engine", "usuarios", "modo", "query_id", "count", "mean", "p50", "p95", "p99", "result_rows_median"]
        lines.append(dataframe_to_markdown(queries[cols].sort_values(["query_id", "engine", "usuarios"])))
    lines.extend(["", "## Armazenamento", ""])
    if storage.empty:
        lines.append("Sem metricas de armazenamento coletadas.")
    else:
        lines.append(dataframe_to_markdown(storage))
    lines.extend(
        [
            "",
            "## Observacoes",
            "",
            "- Os resultados ClickHouse disponiveis possuem ingestao completa e consultas sequenciais.",
            "- A comparacao filtra consultas Trino com cardinalidade final equivalente ao ClickHouse.",
            "- Para a comparacao final do trabalho, use `results/clickhouse_fixed` e `results/trino_aws_fixed`.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_charts(ingestion: pd.DataFrame, queries: pd.DataFrame, out_dir: Path) -> None:
    if not ingestion.empty:
        fig, ax = plt.subplots(figsize=(8, 4.5))
        data = ingestion.groupby("engine")["linhas_por_segundo"].mean().sort_values()
        data.plot(kind="bar", ax=ax, color=["#4c78a8", "#f58518"][: len(data)])
        ax.set_title("Throughput medio de ingestao")
        ax.set_ylabel("linhas/segundo")
        ax.set_xlabel("")
        fig.tight_layout()
        fig.savefig(out_dir / "ingestion_throughput.png", dpi=160)
        plt.close(fig)

    if not queries.empty:
        q = queries.copy()
        fig, ax = plt.subplots(figsize=(10, 5))
        labels = q["engine"] + " u" + q["usuarios"].astype(str) + " " + q["query_id"]
        ax.bar(labels, q["p95"], color="#54a24b")
        ax.set_title("Latencia p95 por consulta")
        ax.set_ylabel("segundos")
        ax.tick_params(axis="x", rotation=70)
        fig.tight_layout()
        fig.savefig(out_dir / "query_p95_latency.png", dpi=160)
        plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Gera comparacao DW ClickHouse vs Lakehouse Trino/Iceberg.")
    parser.add_argument("--skip-storage", action="store_true")
    parser.add_argument("--clickhouse-dir", default=str(CLICKHOUSE_DIR))
    parser.add_argument("--trino-dir", default=str(TRINO_DIR))
    parser.add_argument("--output-dir", default=str(COMPARE_DIR))
    args = parser.parse_args()

    clickhouse_dir = Path(args.clickhouse_dir)
    trino_dir = Path(args.trino_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ingestion = summarize_ingestion(clickhouse_dir, trino_dir)
    queries = summarize_queries(clickhouse_dir, trino_dir)
    storage = pd.DataFrame() if args.skip_storage else collect_trino_storage(trino_dir)

    ingestion.to_csv(output_dir / "ingestion_summary.csv", index=False)
    queries.to_csv(output_dir / "query_comparison_summary.csv", index=False)
    storage.to_csv(output_dir / "storage_summary.csv", index=False)
    write_markdown(ingestion, queries, storage, output_dir / "comparison_report.md")
    make_charts(ingestion, queries, output_dir)
    logger.info("Comparacao gravada em %s", output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
