import os
import sys
import time
import logging
import concurrent.futures
import clickhouse_connect
from dotenv import load_dotenv

# Load configuration from the absolute path of the .env file
load_dotenv("c:/Users/lucas/dev/unb-bdm/.env")

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Thread-%(thread)d] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("siorg_parallel_benchmark.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("siorg_parallel_benchmarker")

# ClickHouse Configuration
ch_host = os.getenv("CLICKHOUSE_HOST")
ch_port_str = os.getenv("CLICKHOUSE_PORT", "8443")
ch_username = os.getenv("CLICKHOUSE_USERNAME", "default")
ch_password = os.getenv("CLICKHOUSE_PASSWORD")
ch_database = os.getenv("CLICKHOUSE_DATABASE", "default")

# Check required configurations
if not ch_host or not ch_password:
    logger.error("ClickHouse credentials not configured in the .env file.")
    sys.exit(1)

try:
    ch_port = int(ch_port_str)
except ValueError:
    logger.error(f"Invalid ClickHouse port '{ch_port_str}'. Must be an integer.")
    sys.exit(1)

# Analytical Queries Definitions (No trailing semicolons)
queries = {
    "Query 1: Expansão do Estado (Window Functions)": """
        SELECT
            ano_referencia,
            mes_referencia,
            count(DISTINCT codigoUnidade) AS total_unidades,
            count(DISTINCT codigoOrgaoEntidade) AS total_orgaos,
            total_unidades - lagInFrame(total_unidades, 1, 0) OVER (ORDER BY ano_referencia, mes_referencia) AS variacao_liquida_unidades
        FROM estrutura_organizacional_completa
        GROUP BY ano_referencia, mes_referencia
        ORDER BY ano_referencia, mes_referencia
    """,
    "Query 2: Relocalizações Geográficas de Setores (Self-Join Histórico)": """
        SELECT
            A.ano_referencia AS ano_anterior,
            A.mes_referencia AS mes_anterior,
            B.ano_referencia AS ano_atual,
            B.mes_referencia AS mes_atual,
            A.nome_orgao_entidade,
            A.nome_unidade,
            A.municipio AS municipio_origem,
            A.uf AS uf_origem,
            B.municipio AS municipio_destino,
            B.uf AS uf_destino
        FROM distribuicao_orgaos AS A
        INNER JOIN distribuicao_orgaos AS B 
            ON A.cod_unidade = B.cod_unidade
           AND B.ano_referencia = (A.mes_referencia == 12 ? A.ano_referencia + 1 : A.ano_referencia)
           AND B.mes_referencia = (A.mes_referencia == 12 ? 1 : A.mes_referencia + 1)
        WHERE A.municipio != B.municipio OR A.uf != B.uf
        ORDER BY A.nome_orgao_entidade, A.ano_referencia, A.mes_referencia
    """,
    "Query 3: Série Histórica de Complexidade (Agrupamento por Órgão)": """
        SELECT
            nome_orgao_entidade,
            sigla_orgao_entidade,
            ano_referencia,
            mes_referencia,
            count(DISTINCT cod_unidade) AS total_setores,
            max(toUInt8OrZero(nivel_hierarquico)) AS profundidade_maxima_hierarquia,
            round(avg(toUInt8OrZero(nivel_hierarquico)), 2) AS nivel_hierarquico_medio
        FROM distribuicao_orgaos
        WHERE nome_orgao_entidade IS NOT NULL 
          AND nome_orgao_entidade != ''
        GROUP BY 
            nome_orgao_entidade, 
            sigla_orgao_entidade, 
            ano_referencia, 
            mes_referencia
        ORDER BY 
            nome_orgao_entidade, 
            ano_referencia, 
            mes_referencia
    """
}

def execute_and_profile_query(query_name, sql_text, execution_id, concurrency_level, thread_index):
    """
    Executes and profiles a query in thread-isolated environment
    and logs the metrics directly into ClickHouse log_consultas table.
    """
    try:
        # Each thread gets its own ClickHouse connection client (thread-safe)
        client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_username,
            password=ch_password,
            database=ch_database,
            secure=True
        )
    except Exception as conn_err:
        logger.error(f"Thread-{thread_index} failed to connect to ClickHouse: {conn_err}")
        return None

    t_start = time.time()
    try:
        # Run query
        res = client.query(sql_text)
        t_end = time.time()
        client_duration = t_end - t_start
        
        # Get query execution metadata
        query_id = res.query_id or "N/A"
        read_rows = int(res.summary.get('read_rows', 0))
        read_bytes = int(res.summary.get('read_bytes', 0))
        
        elapsed_seconds = float(res.summary.get('elapsed', client_duration))
        if elapsed_seconds == 0.0:
            elapsed_seconds = client_duration
            
        result_rows = int(len(res.result_set))
        
        logger.info(
            f"Thread-{thread_index} SUCCESS | Query: '{query_name}' | "
            f"Execution ID: {execution_id} | Concurrency: {concurrency_level} | "
            f"Time: {elapsed_seconds:.4f}s | Scanned: {read_rows:,} rows"
        )
        
        # Insert performance metrics into log_consultas
        client.insert(
            "log_consultas",
            [[execution_id, concurrency_level, query_name, query_id, elapsed_seconds, read_rows, read_bytes, result_rows, sql_text]],
            column_names=[
                "id_execucao", "concurrency_level", "query_name", "query_id", 
                "elapsed_seconds", "read_rows", "read_bytes", "result_rows", "query_text"
            ]
        )
        client.close()
        return {
            "query_name": query_name,
            "elapsed_seconds": elapsed_seconds,
            "read_rows": read_rows,
            "read_bytes": read_bytes,
            "result_rows": result_rows
        }
    except Exception as query_err:
        logger.error(f"Thread-{thread_index} failed executing query '{query_name}': {query_err}")
        client.close()
        return None

def run_concurrency_test():
    # 1. Fetch current max execution ID and increment it once for the entire benchmark run
    logger.info(f"Connecting to ClickHouse Cloud to fetch next Execution ID...")
    try:
        init_client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_username,
            password=ch_password,
            database=ch_database,
            secure=True
        )
        res_max = init_client.query("SELECT max(id_execucao) FROM log_consultas")
        max_id = res_max.result_set[0][0]
        execution_id = int((max_id or 0) + 1)
        init_client.close()
    except Exception as e:
        logger.error(f"Failed to fetch next execution_id: {e}")
        sys.exit(1)

    logger.info(f"\n==========================================")
    logger.info(f"STARTING PARALLEL BENCHMARK (Run ID: {execution_id})")
    logger.info(f"==========================================\n")

    concurrency_levels = [1, 5, 10, 20]
    all_results = {}

    for c in concurrency_levels:
        logger.info(f"--- Concurrency Level: {c} Parallel Queries ---")
        all_results[c] = {}
        
        for q_name, q_sql in queries.items():
            logger.info(f"Spawning {c} parallel threads for '{q_name}'...")
            
            # Setup ThreadPoolExecutor for concurrent runs
            with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
                # Submit c threads concurrently
                futures = [
                    executor.submit(execute_and_profile_query, q_name, q_sql, execution_id, c, i)
                    for i in range(1, c + 1)
                ]
                # Wait for all submitted threads to finish
                results = [f.result() for f in concurrent.futures.as_completed(futures)]
            
            # Filter successful threads and aggregate metrics
            valid_results = [r for r in results if r is not None]
            if valid_results:
                avg_time = sum(r['elapsed_seconds'] for r in valid_results) / len(valid_results)
                total_scanned_rows = sum(r['read_rows'] for r in valid_results)
                total_scanned_mb = sum(r['read_bytes'] for r in valid_results) / (1024 * 1024)
                
                all_results[c][q_name] = {
                    "avg_time": avg_time,
                    "total_scanned_rows": total_scanned_rows,
                    "total_scanned_mb": total_scanned_mb,
                    "successful_threads": len(valid_results)
                }
            else:
                all_results[c][q_name] = {
                    "avg_time": 0.0,
                    "total_scanned_rows": 0,
                    "total_scanned_mb": 0.0,
                    "successful_threads": 0
                }
            logger.info(f"Completed '{q_name}' at Concurrency {c}.\n")

    # Output detailed scientific summary
    logger.info(f"\n==========================================")
    logger.info(f"SCIENTIFIC PERFORMANCE SUMMARY (Run ID: {execution_id})")
    logger.info(f"==========================================")
    for q_name in queries.keys():
        logger.info(f"\nQuery: {q_name}")
        logger.info(f"| Concurrency | Successful Runs | Avg DB Time (s) | Total Rows Scanned | Total Data Scanned |")
        logger.info(f"|-------------|-----------------|-----------------|--------------------|--------------------|")
        for c in concurrency_levels:
            stats = all_results[c].get(q_name, {})
            logger.info(
                f"| {c:<11} | {stats.get('successful_threads', 0):<15} | "
                f"{stats.get('avg_time', 0.0):.4f}s          | "
                f"{stats.get('total_scanned_rows', 0):<18,} | "
                f"{stats.get('total_scanned_mb', 0.0):.2f} MB          |"
            )
    logger.info(f"\n==========================================\n")
    logger.info("Parallel Query Benchmarking Suite complete.")

if __name__ == "__main__":
    run_concurrency_test()
