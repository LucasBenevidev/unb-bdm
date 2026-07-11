import os
import sys
import time
import clickhouse_connect
from dotenv import load_dotenv

# Load configuration from the absolute path of the .env file
load_dotenv("c:/Users/lucas/dev/unb-bdm/.env")

# ClickHouse Configuration
ch_host = os.getenv("CLICKHOUSE_HOST")
ch_port_str = os.getenv("CLICKHOUSE_PORT", "8443")
ch_username = os.getenv("CLICKHOUSE_USERNAME", "default")
ch_password = os.getenv("CLICKHOUSE_PASSWORD")
ch_database = os.getenv("CLICKHOUSE_DATABASE", "default")

# Check required configurations
if not ch_host or not ch_password:
    print("Error: ClickHouse credentials not configured in the .env file.")
    sys.exit(1)

try:
    ch_port = int(ch_port_str)
except ValueError:
    print(f"Error: Invalid ClickHouse port '{ch_port_str}'. Must be an integer.")
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
    print(f"Connecting to ClickHouse Cloud at {ch_host}:{ch_port}...")
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
        print(f"Connection failed: {e}")
        sys.exit(1)

    print("\n--- Starting Query Performance Benchmark ---\n")

    for name, sql_text in queries.items():
        print(f"Executing: {name}...")
        
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
            
            print(f"  Query ID: {query_id}")
            print(f"  Duration: {elapsed_seconds:.4f} seconds (Client-measured: {client_duration:.4f}s)")
            print(f"  Scanned: {read_rows:,} rows ({read_bytes / (1024*1024):.2f} MB)")
            print(f"  Result set size: {result_rows:,} rows")
            
            # Insert statistics into log_consultas
            client.insert(
                "log_consultas",
                [[name, query_id, elapsed_seconds, read_rows, read_bytes, result_rows, sql_text]],
                column_names=["query_name", "query_id", "elapsed_seconds", "read_rows", "read_bytes", "result_rows", "query_text"]
            )
            print("  Metrics logged successfully.\n")
            
        except Exception as e:
            print(f"  Error executing query: {e}\n")

    print("--- Benchmark execution complete ---")
    client.close()

if __name__ == "__main__":
    run_benchmark()
