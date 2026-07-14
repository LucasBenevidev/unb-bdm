from __future__ import annotations

import argparse
import csv
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

from trino.exceptions import TrinoConnectionError, TrinoUserError

from siorg_trino_common import (
    ENGINE,
    ensure_results_dir,
    get_trino_connection,
    qualified_table,
    quote_identifier,
    setup_logging,
    write_csv_rows,
)


logger = setup_logging("benchmark_trino_queries", "siorg_trino_benchmark.log")

QUERY_LOG_FIELDS = [
    "id_execucao",
    "data_execucao",
    "engine",
    "query_id",
    "query_name",
    "trino_query_id",
    "duracao_segundos",
    "linhas_retornadas",
    "usuarios",
    "modo",
    "status",
    "erro",
]

SUMMARY_FIELDS = [
    "id_execucao",
    "engine",
    "query_id",
    "query_name",
    "usuarios",
    "modo",
    "count",
    "mean",
    "min",
    "max",
    "p50",
    "p95",
    "p99",
    "erros",
]

QUERIES = [
    {
        "id": "q01",
        "name": "Expansao do Estado (Window Functions)",
        "sql": """
            SELECT
                ano_referencia,
                mes_referencia,
                count(DISTINCT "codigoUnidade") AS total_unidades,
                count(DISTINCT "codigoOrgaoEntidade") AS total_orgaos,
                count(DISTINCT "codigoUnidade")
                    - lag(count(DISTINCT "codigoUnidade"), 1, 0)
                      OVER (ORDER BY ano_referencia, mes_referencia) AS variacao_liquida_unidades
            FROM iceberg.siorg.estrutura_organizacional_completa
            GROUP BY ano_referencia, mes_referencia
            ORDER BY ano_referencia, mes_referencia
        """,
    },
    {
        "id": "q02",
        "name": "Relocalizacoes Geograficas de Setores (Self-Join Historico)",
        "sql": """
            SELECT
                a.ano_referencia AS ano_anterior,
                a.mes_referencia AS mes_anterior,
                b.ano_referencia AS ano_atual,
                b.mes_referencia AS mes_atual,
                a.nome_orgao_entidade,
                a.nome_unidade,
                a.municipio AS municipio_origem,
                a.uf AS uf_origem,
                b.municipio AS municipio_destino,
                b.uf AS uf_destino
            FROM iceberg.siorg.distribuicao_orgaos AS a
            INNER JOIN iceberg.siorg.distribuicao_orgaos AS b
                ON a.cod_unidade = b.cod_unidade
               AND b.ano_referencia = CASE WHEN a.mes_referencia = 12 THEN a.ano_referencia + 1 ELSE a.ano_referencia END
               AND b.mes_referencia = CASE WHEN a.mes_referencia = 12 THEN 1 ELSE a.mes_referencia + 1 END
            WHERE a.municipio <> b.municipio OR a.uf <> b.uf
            ORDER BY a.nome_orgao_entidade, a.ano_referencia, a.mes_referencia
        """,
    },
    {
        "id": "q03",
        "name": "Serie Historica de Complexidade (Agrupamento por Orgao)",
        "sql": """
            SELECT
                nome_orgao_entidade,
                sigla_orgao_entidade,
                ano_referencia,
                mes_referencia,
                count(DISTINCT cod_unidade) AS total_setores,
                max(COALESCE(try_cast(nivel_hierarquico AS integer), 0)) AS profundidade_maxima_hierarquia,
                round(avg(COALESCE(try_cast(nivel_hierarquico AS integer), 0)), 2) AS nivel_hierarquico_medio
            FROM iceberg.siorg.distribuicao_orgaos
            WHERE nome_orgao_entidade IS NOT NULL
              AND nome_orgao_entidade <> ''
            GROUP BY
                nome_orgao_entidade,
                sigla_orgao_entidade,
                ano_referencia,
                mes_referencia
            ORDER BY
                nome_orgao_entidade,
                ano_referencia,
                mes_referencia
        """,
    },
]


def next_execution_id() -> int:
    csv_path = ensure_results_dir() / "query_log.csv"
    if csv_path.exists():
        try:
            with csv_path.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            ids = [int(row["id_execucao"]) for row in rows if row.get("id_execucao")]
            if ids:
                return max(ids) + 1
        except (OSError, ValueError, KeyError) as exc:
            logger.warning("Nao foi possivel calcular id_execucao pelo CSV local: %s", exc)
    return 1


def trino_query_id(cur) -> str:
    stats = getattr(cur, "stats", None) or {}
    return str(stats.get("queryId") or stats.get("query_id") or "")


def execute_query(query: dict[str, str], id_execucao: int, users: int, mode: str) -> dict[str, Any]:
    start = time.perf_counter()
    data_execucao = datetime.now().isoformat(sep=" ", timespec="seconds")
    status = "success"
    error = ""
    rows_returned = 0
    trino_id = ""
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute(query["sql"])
        rows = cur.fetchall()
        rows_returned = len(rows)
        trino_id = trino_query_id(cur)
        conn.close()
    except (TrinoUserError, TrinoConnectionError, OSError) as exc:
        status = "error"
        error = str(exc)
    duration = time.perf_counter() - start
    return {
        "id_execucao": id_execucao,
        "data_execucao": data_execucao,
        "engine": ENGINE,
        "query_id": query["id"],
        "query_name": query["name"],
        "trino_query_id": trino_id,
        "duracao_segundos": round(duration, 6),
        "linhas_retornadas": rows_returned,
        "usuarios": users,
        "modo": mode,
        "status": status,
        "erro": error,
    }


def insert_query_log(row: dict[str, Any]) -> None:
    conn = get_trino_connection()
    cur = conn.cursor()
    quoted = ", ".join(quote_identifier(column) for column in QUERY_LOG_FIELDS)
    casts = [
        "CAST(? AS INTEGER)",
        "CAST(? AS TIMESTAMP(6))",
        "CAST(? AS VARCHAR)",
        "CAST(? AS VARCHAR)",
        "CAST(? AS VARCHAR)",
        "CAST(? AS VARCHAR)",
        "CAST(? AS DOUBLE)",
        "CAST(? AS BIGINT)",
        "CAST(? AS INTEGER)",
        "CAST(? AS VARCHAR)",
        "CAST(? AS VARCHAR)",
        "CAST(? AS VARCHAR)",
    ]
    cur.execute(
        f"INSERT INTO {qualified_table('query_log')} ({quoted}) SELECT {', '.join(casts)}",
        [row[column] for column in QUERY_LOG_FIELDS],
    )
    conn.close()


def worker(
    user_index: int,
    workload: list[dict[str, str]],
    barrier: threading.Barrier,
    id_execucao: int,
    users: int,
    mode: str,
) -> list[dict[str, Any]]:
    conn = get_trino_connection()
    conn.close()
    barrier.wait()
    results = []
    for query in workload:
        try:
            results.append(execute_query(query, id_execucao, users, mode))
        except Exception as exc:
            results.append(
                {
                    "id_execucao": id_execucao,
                    "data_execucao": datetime.now().isoformat(sep=" ", timespec="seconds"),
                    "engine": ENGINE,
                    "query_id": query["id"],
                    "query_name": query["name"],
                    "trino_query_id": "",
                    "duracao_segundos": 0,
                    "linhas_retornadas": 0,
                    "usuarios": users,
                    "modo": mode,
                    "status": "error",
                    "erro": str(exc),
                }
            )
    logger.info("Usuario virtual %s finalizou %s consulta(s).", user_index, len(workload))
    return results


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((pct / 100) * (len(ordered) - 1))))
    return ordered[index]


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, str, str, int, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            int(row["id_execucao"]),
            row["query_id"],
            row["query_name"],
            int(row["usuarios"]),
            row["modo"],
        )
        grouped.setdefault(key, []).append(row)
    summary = []
    for (id_execucao, query_id, query_name, users, mode), items in grouped.items():
        durations = [float(item["duracao_segundos"]) for item in items if item["status"] == "success"]
        errors = sum(1 for item in items if item["status"] != "success")
        summary.append(
            {
                "id_execucao": id_execucao,
                "engine": ENGINE,
                "query_id": query_id,
                "query_name": query_name,
                "usuarios": users,
                "modo": mode,
                "count": len(durations),
                "mean": round(statistics.mean(durations), 6) if durations else 0,
                "min": round(min(durations), 6) if durations else 0,
                "max": round(max(durations), 6) if durations else 0,
                "p50": round(percentile(durations, 50), 6),
                "p95": round(percentile(durations, 95), 6),
                "p99": round(percentile(durations, 99), 6),
                "erros": errors,
            }
        )
    return summary


def build_workloads(users: int, repetitions: int, mode: str, seed: int, same_query_id: str) -> list[list[dict[str, str]]]:
    if users == 1:
        return [QUERIES * repetitions]
    if mode == "same-query":
        selected = next((query for query in QUERIES if query["id"] == same_query_id), QUERIES[0])
        return [[selected] * repetitions for _ in range(users)]
    rng = random.Random(seed)
    workloads = []
    for index in range(users):
        sequence = QUERIES * repetitions
        rng.shuffle(sequence)
        if index % 2 == 0:
            sequence = list(reversed(sequence))
        workloads.append(sequence)
    return workloads


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark Trino/Iceberg das consultas SIORG.")
    parser.add_argument("--users", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--mode", choices=["same-query", "mixed-workload"], default="mixed-workload")
    parser.add_argument("--same-query-id", choices=[query["id"] for query in QUERIES], default="q01")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip-iceberg-log", action="store_true", help="Grava apenas CSV local de resultados.")
    args = parser.parse_args()

    id_execucao = next_execution_id()
    logger.info(
        "Iniciando benchmark id_execucao=%s usuarios=%s modo=%s repeticoes=%s",
        id_execucao,
        args.users,
        args.mode,
        args.repetitions,
    )

    workloads = build_workloads(args.users, args.repetitions, args.mode, args.seed, args.same_query_id)
    barrier = threading.Barrier(args.users)
    rows: list[dict[str, Any]] = []
    if args.users == 1:
        rows.extend(worker(1, workloads[0], barrier, id_execucao, args.users, args.mode))
    else:
        with ThreadPoolExecutor(max_workers=args.users) as executor:
            futures = [
                executor.submit(worker, index + 1, workload, barrier, id_execucao, args.users, args.mode)
                for index, workload in enumerate(workloads)
            ]
            for future in as_completed(futures):
                try:
                    rows.extend(future.result())
                except Exception as exc:
                    logger.error("Worker falhou antes de retornar logs: %s", exc)

    output_dir = ensure_results_dir()
    write_csv_rows(output_dir / "query_log.csv", QUERY_LOG_FIELDS, rows)
    if not args.skip_iceberg_log:
        for row in rows:
            try:
                insert_query_log(row)
            except (TrinoUserError, TrinoConnectionError, OSError) as exc:
                logger.error("Falha ao gravar query_log no Iceberg: %s", exc)

    query_log_path = output_dir / "query_log.csv"
    with query_log_path.open("r", newline="", encoding="utf-8") as handle:
        all_rows = list(csv.DictReader(handle))
    summary_rows = summarize(all_rows)
    summary_path = output_dir / "query_summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(summary_rows)
    logger.info("Logs gravados em %s", output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
