import os
import sys
import time
import re
import csv
import logging
import boto3
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
        logging.FileHandler("logs/siorg_load.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("siorg_loader")

# S3 Configuration
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
bucket_name = os.getenv("S3_BUCKET_NAME", "unb-bdm-siorg")

# ClickHouse Configuration
ch_host = os.getenv("CLICKHOUSE_HOST")
ch_port_str = os.getenv("CLICKHOUSE_PORT", "8443")
ch_username = os.getenv("CLICKHOUSE_USERNAME", "default")
ch_password = os.getenv("CLICKHOUSE_PASSWORD")
ch_database = os.getenv("CLICKHOUSE_DATABASE", "default")
truncate_before_load = os.getenv("TRUNCATE_BEFORE_LOAD", "false").lower() == "true"
logger.info(f"TRUNCATE_BEFORE_LOAD raw env value: '{os.getenv('TRUNCATE_BEFORE_LOAD')}'")
logger.info(f"truncate_before_load parsed boolean: {truncate_before_load}")

# Check required configurations
if not aws_access_key or not aws_secret_key:
    logger.error("AWS credentials not configured in environment/.env file.")
    sys.exit(1)

if not ch_host or not ch_password:
    logger.error("ClickHouse credentials not configured in environment/.env file.")
    sys.exit(1)

try:
    ch_port = int(ch_port_str)
except ValueError:
    logger.error(f"Invalid ClickHouse port '{ch_port_str}'. Must be an integer.")
    sys.exit(1)

# ClickHouse Table Columns (excluding DWH audit/metadata columns which are appended by Python/SQL)
DISTRIBUICAO_CSV_COLS = [
    'nivel_hierarquico', 'cod_orgao_entidade', 'nome_orgao_entidade', 'sigla_orgao_entidade',
    'natureza_juridica', 'subnatureza_juridica', 'poder', 'esfera', 'cod_unidade_pai', 'cod_unidade',
    'nome_unidade', 'sigla_unidade', 'endereco', 'endereco_complemento', 'bairro', 'municipio', 'uf',
    'cep', 'telefone', 'email', 'area_atuacao', 'nivel_normatizacao', 'autonomia_gestao_cargos',
    'tipo_cargo', 'categoria_cargo', 'nivel_cargo', 'quantidade', 'denominacao_cargo',
    'complemento_denominacao', 'mobilidade', 'obriga_distribuicao', 'compoe_estrutura', 'autoridade',
    'regra_autoridade', 'regra_cargo_nome_unidade', 'temporario', 'cod_instancia'
]

COMPLETA_CSV_COLS = [
    'codigoUnidade', 'codigoUnidadePai', 'codigoOrgaoEntidade', 'codigoTipoUnidade', 'nome', 'sigla',
    'codigoEsfera', 'codigoPoder', 'codigoNaturezaJuridica', 'codigoSubNaturezaJuridica', 'nivelNormatizacao',
    'versaoConsulta', 'dataInicialVersaoConsulta', 'dataFinalVersaoConsulta', 'operacao',
    'codigoUnidadePaiAnterior', 'codigoOrgaoEntidadeAnterior', 'regulamentoEspecifico', 'codigoCategoriaUnidade',
    'competencia', 'finalidade', 'missao', 'descricaoAtoNormativo', 'areaAtuacao', 'telefone', 'email', 'site',
    'linhaEndereco', 'bairro', 'cep', 'uf', 'municipio', 'pais', 'horarioFuncionamento'
]

def create_log_table_if_not_exists(ch_client):
    """Create execution log table in ClickHouse"""
    logger.info("Ensuring metadata table 'log_cargas' exists and has the execution ID column...")
    query = """
    CREATE TABLE IF NOT EXISTS log_cargas (
        data_execucao DateTime DEFAULT now(),
        id_execucao UInt32,
        nome_arquivo String,
        tempo_carga Float64,
        total_linhas UInt64,
        linhas_por_segundo Float64
    ) ENGINE = MergeTree()
    ORDER BY data_execucao;
    """
    ch_client.command(query)
    # Safely alter the table in case it was created without the id_execucao column previously
    ch_client.command("ALTER TABLE log_cargas ADD COLUMN IF NOT EXISTS id_execucao UInt32")

def truncate_tables_if_requested(ch_client):
    """Truncate data staging tables only (preserving log_cargas table)"""
    if truncate_before_load:
        logger.info("TRUNCATE_BEFORE_LOAD=true detected. Truncating data tables (preserving log_cargas history)...")
        ch_client.command("TRUNCATE TABLE IF EXISTS distribuicao_orgaos")
        ch_client.command("TRUNCATE TABLE IF EXISTS estrutura_organizacional_completa")
        logger.info("Data tables truncated successfully.")

def file_already_loaded(ch_client, filename):
    """Check if the CSV file was already loaded successfully in ClickHouse"""
    res = ch_client.query("SELECT count() FROM log_cargas WHERE nome_arquivo = %s", [filename])
    return res.result_set[0][0] > 0

def log_execution(ch_client, filename, duration, rows_count, rows_per_sec, execution_id):
    """Insert loading metadata into ClickHouse"""
    logger.info(f"Saving load stats for '{filename}' to log_cargas (Execution ID: {execution_id})...")
    ch_client.insert(
        "log_cargas",
        [[execution_id, filename, duration, rows_count, rows_per_sec]],
        column_names=["id_execucao", "nome_arquivo", "tempo_carga", "total_linhas", "linhas_por_segundo"]
    )

def load_csv_file(ch_client, s3_client, s3_key, ch_table, target_csv_cols, execution_id):
    """Run native ClickHouse S3 loading query for the file"""
    filename = os.path.basename(s3_key)
    
    # Extract reference year and month from filename
    match = re.search(r'-(\d{4})-(\d{2})\.csv', filename)
    if not match:
        logger.warning(f"Skipping file '{filename}'. Could not parse YYYY-MM snapshot date from filename.")
        return
        
    year = int(match.group(1))
    month = int(match.group(2))
    
    # Skip if already loaded, unless TRUNCATE_BEFORE_LOAD=true is enabled
    if not truncate_before_load and file_already_loaded(ch_client, filename):
        logger.info(f"File '{filename}' has already been loaded previously. Skipping.")
        return
        
    logger.info(f"Starting load for '{filename}' (Snapshot Reference Date: {year:04d}-{month:02d})")
    
    # 1. Fetch the first 4KB of the S3 object to parse the headers (takes < 50ms)
    logger.info(f"Fetching CSV header from S3 for column matching...")
    try:
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_key, Range="bytes=0-4096")
        first_bytes = resp['Body'].read().decode('utf-8', errors='ignore')
        header_row = next(csv.reader(first_bytes.splitlines()))
    except Exception as e:
        logger.error(f"Failed to read CSV header from S3 for '{filename}': {e}")
        return

    # Clean up column names in header (remove BOM or spaces if any)
    cleaned_header = [col.strip().replace('\ufeff', '') for col in header_row]

    # Filter columns to only include those present in both the CSV header and our table definition
    valid_cols = [col for col in cleaned_header if col in target_csv_cols]
    if not valid_cols:
        logger.error(f"No valid matching columns found between CSV header and database schema for '{filename}'.")
        return

    # Construct regional S3 endpoint url
    s3_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"

    # Construct high-performance INSERT SELECT query using ClickHouse s3 function
    insert_cols = ", ".join(valid_cols + ["ano_referencia", "mes_referencia"])
    select_cols = ", ".join(valid_cols + [f"{year} AS ano_referencia", f"{month} AS mes_referencia"])

    query = f"""
    INSERT INTO {ch_table} ({insert_cols})
    SELECT {select_cols}
    FROM s3(
        '{s3_url}',
        '{aws_access_key}',
        '{aws_secret_key}',
        'CSVWithNames'
    )
    """

    start_time = time.time()
    
    logger.info("Executing native S3 loading query inside ClickHouse Cloud...")
    try:
        ch_client.command(query)
    except Exception as e:
        logger.error(f"ClickHouse S3 loading query failed for '{filename}': {e}")
        return

    end_time = time.time()
    duration = end_time - start_time
    
    # Query ClickHouse to find out how many rows were loaded for this snapshot
    count_res = ch_client.query(
        f"SELECT count() FROM {ch_table} WHERE ano_referencia = %s AND mes_referencia = %s",
        [year, month]
    )
    rows_count = count_res.result_set[0][0]
    rows_per_sec = rows_count / duration if duration > 0 else 0
    
    logger.info(f"SUCCESS: Loaded {rows_count} rows from '{filename}' into '{ch_table}' in {duration:.2f}s ({rows_per_sec:.2f} rows/sec)")
    
    # 3. Log metadata in ClickHouse
    log_execution(ch_client, filename, duration, rows_count, rows_per_sec, execution_id)

def main():
    logger.info("Initializing S3 and ClickHouse Cloud connections...")
    
    try:
        # Initialize boto3 S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
        
        # Initialize ClickHouse connection
        ch_client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_username,
            password=ch_password,
            database=ch_database,
            secure=True
        )
        
        # Ensure log table exists
        create_log_table_if_not_exists(ch_client)
        
        # Get the next execution run ID (sequential integer)
        res = ch_client.query("SELECT max(id_execucao) FROM log_cargas")
        max_id = res.result_set[0][0]
        execution_id = int((max_id or 0) + 1)
        logger.info(f"Assigned Execution ID: {execution_id} for this loading run.")
        
        # Truncate tables if requested
        truncate_tables_if_requested(ch_client)
        
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        sys.exit(1)
        
    # List files in S3 under bucket prefix
    logger.info(f"Scanning S3 Bucket '{bucket_name}' for CSV files...")
    s3_files = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv'):
                    s3_files.append(key)
    except Exception as e:
        logger.error(f"Failed to scan S3 bucket: {e}")
        sys.exit(1)
        
    logger.info(f"Found {len(s3_files)} CSV files in S3.")
    
    # Sort files to ensure chronological order loading if needed
    s3_files.sort()
    
    # Process files
    for key in s3_files:
        filename = os.path.basename(key)
        
        if "estrutura-organizacional-completa" in key:
            ch_table = "estrutura_organizacional_completa"
            target_csv_cols = COMPLETA_CSV_COLS
        elif "distribuicao" in key:
            ch_table = "distribuicao_orgaos"
            target_csv_cols = DISTRIBUICAO_CSV_COLS
        else:
            logger.info(f"Ignoring file '{filename}' as it does not match known categories.")
            continue
            
        try:
            load_csv_file(ch_client, s3_client, key, ch_table, target_csv_cols, execution_id)
        except Exception as ex:
            logger.error(f"Error loading '{filename}': {ex}")
            
    logger.info("Execution complete.")
    ch_client.close()

if __name__ == "__main__":
    main()
