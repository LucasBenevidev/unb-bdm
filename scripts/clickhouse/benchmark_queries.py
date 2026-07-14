import os
import sys
import time
import logging
import clickhouse_connect
from dotenv import load_dotenv

# Load configuration from the repository .env file.
load_dotenv(".env")
os.makedirs("logs", exist_ok=True)

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/siorg_benchmark.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("siorg_benchmarker")

# ClickHouse Configuration
ch_host = os.getenv("CLICKHOUSE_HOST")
ch_port_str = os.getenv("CLICKHOUSE_PORT", "8443")
ch_username = os.getenv("CLICKHOUSE_USERNAME", "default")
ch_password = os.getenv("CLICKHOUSE_PASSWORD")
ch_database = os.getenv("CLICKHOUSE_DATABASE", "default")
benchmark_runs_str = os.getenv("BENCHMARK_RUNS", "1")

# Check required configurations
if not ch_host or not ch_password:
    logger.error("ClickHouse credentials not configured in the .env file.")
    sys.exit(1)

try:
    ch_port = int(ch_port_str)
except ValueError:
    logger.error(f"Invalid ClickHouse port '{ch_port_str}'. Must be an integer.")
    sys.exit(1)

try:
    benchmark_runs = int(benchmark_runs_str)
except ValueError:
    logger.error(f"Invalid BENCHMARK_RUNS '{benchmark_runs_str}'. Must be an integer.")
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

def run_benchmark():
    logger.info(f"Connecting to ClickHouse Cloud at {ch_host}:{ch_port}...")
    try:
        client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_username,
            password=ch_password,
            database=ch_database,
            secure=True
        )
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        sys.exit(1)

    logger.info(f"--- Starting Query Performance Benchmark (Runs: {benchmark_runs}) ---\n")

    for run in range(1, benchmark_runs + 1):
        # 1. Query current max execution ID to compute the next execution_id for this loop run
        try:
            res_max = client.query("SELECT max(id_execucao) FROM log_consultas")
            max_id = res_max.result_set[0][0]
            execution_id = int((max_id or 0) + 1)
        except Exception as e:
            logger.error(f"Failed to fetch max execution_id: {e}")
            client.close()
            sys.exit(1)

        logger.info(f"=== LOOP ITERATION {run}/{benchmark_runs} (Assigned Execution ID: {execution_id}) ===")

        for name, sql_text in queries.items():
            logger.info(f"Executing: {name}...")
            
            # Client-side time measurement
            t_start = time.time()
            
            try:
                # Run query
                res = client.query(sql_text)
                
                # Client-side end time
                t_end = time.time()
                client_duration = t_end - t_start
                
                # Get stats directly from ClickHouse response summary
                query_id = res.query_id or "N/A"
                
                # Explicitly cast metrics to integers to prevent formatting errors
                read_rows = int(res.summary.get('read_rows', 0))
                read_bytes = int(res.summary.get('read_bytes', 0))
                
                # ClickHouse engine execution time (fallback to client-side if missing)
                elapsed_seconds = float(res.summary.get('elapsed', client_duration))
                if elapsed_seconds == 0.0:
                    elapsed_seconds = client_duration
                    
                result_rows = int(len(res.result_set))
                
                logger.info(f"  Query ID: {query_id}")
                logger.info(f"  Duration: {elapsed_seconds:.4f} seconds (Client-measured: {client_duration:.4f}s)")
                logger.info(f"  Scanned: {read_rows:,} rows ({read_bytes / (1024*1024):.2f} MB)")
                logger.info(f"  Result set size: {result_rows:,} rows")
                
                # Insert statistics into log_consultas
                client.insert(
                    "log_consultas",
                    [[execution_id, name, query_id, elapsed_seconds, read_rows, read_bytes, result_rows, sql_text]],
                    column_names=["id_execucao", "query_name", "query_id", "elapsed_seconds", "read_rows", "read_bytes", "result_rows", "query_text"]
                )
                logger.info("  Metrics logged successfully.\n")
                
            except Exception as e:
                logger.error(f"  Error executing query: {e}\n")

    logger.info("--- Benchmark execution complete ---")
    client.close()

if __name__ == "__main__":
    run_benchmark()
