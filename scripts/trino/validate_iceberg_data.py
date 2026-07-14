from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Any

from trino.exceptions import TrinoUserError

from siorg_trino_common import TABLES, ensure_results_dir, get_trino_connection, qualified_table, setup_logging


logger = setup_logging("validate_iceberg_data")


VALIDATION_FIELDS = ["tabela", "metrica", "valor"]


def scalar(cur, sql: str) -> Any:
    cur.execute(sql)
    return cur.fetchone()[0]


def write_report(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=VALIDATION_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    rows: list[dict[str, Any]] = []
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        for table in TABLES:
            full = qualified_table(table)
            metrics = {
                "total_linhas": f"SELECT count(*) FROM {full}",
                "arquivos_carregados": f"SELECT count(DISTINCT source_file) FROM {full}",
                "competencia_minima": f"SELECT CAST(min(reference_date) AS VARCHAR) FROM {full}",
                "competencia_maxima": f"SELECT CAST(max(reference_date) AS VARCHAR) FROM {full}",
                "source_file_nulo": f"SELECT count(*) FROM {full} WHERE source_file IS NULL",
            }
            if table == "distribuicao_orgaos":
                metrics.update(
                    {
                        "cod_unidade_nulo": f"SELECT count(*) FROM {full} WHERE cod_unidade IS NULL",
                        "duplicidades_cod_unidade_competencia": (
                            f"SELECT count(*) FROM ("
                            f"SELECT cod_unidade, reference_date, count(*) qtd FROM {full} "
                            f"WHERE cod_unidade IS NOT NULL GROUP BY 1, 2 HAVING count(*) > 1)"
                        ),
                    }
                )
            else:
                metrics.update(
                    {
                        "codigoUnidade_nulo": f"SELECT count(*) FROM {full} WHERE \"codigoUnidade\" IS NULL",
                        "duplicidades_codigoUnidade_competencia": (
                            f"SELECT count(*) FROM ("
                            f"SELECT \"codigoUnidade\", reference_date, count(*) qtd FROM {full} "
                            f"WHERE \"codigoUnidade\" IS NOT NULL GROUP BY 1, 2 HAVING count(*) > 1)"
                        ),
                    }
                )
            for metric, sql in metrics.items():
                value = scalar(cur, sql)
                rows.append({"tabela": table, "metrica": metric, "valor": value})
                logger.info("%s.%s = %s", table, metric, value)
        output = ensure_results_dir() / "validation_report.csv"
        write_report(output, rows)
        logger.info("Relatorio de validacao gravado em %s", output)
        conn.close()
        return 0
    except TrinoUserError as exc:
        logger.error("Erro do Trino na validacao: %s", exc)
        return 1
    except OSError as exc:
        logger.error("Falha de conexao com Trino: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

