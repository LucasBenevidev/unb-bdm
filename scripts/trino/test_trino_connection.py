from __future__ import annotations

import sys

from trino.exceptions import TrinoUserError

from siorg_trino_common import get_trino_connection, setup_logging


logger = setup_logging("test_trino_connection")


def main() -> int:
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cur.fetchall()]
        logger.info("Catalogs available: %s", ", ".join(catalogs))
        cur.execute("SHOW TABLES FROM iceberg.siorg")
        tables = [row[0] for row in cur.fetchall()]
        logger.info("Tables in iceberg.siorg: %s", ", ".join(tables) if tables else "(none)")
        cur.execute("SELECT count(*) FROM iceberg.siorg.teste_conexao")
        logger.info("teste_conexao rows: %s", cur.fetchone()[0])
        conn.close()
        return 0
    except TrinoUserError as exc:
        logger.error("Trino returned an error: %s", exc)
        return 1
    except OSError as exc:
        logger.error("Could not connect to Trino: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

