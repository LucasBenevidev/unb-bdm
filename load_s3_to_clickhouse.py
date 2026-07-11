import os
import sys
import time
import re
import csv
import logging
import boto3
import clickhouse_connect
from dotenv import load_dotenv

# Load configuration from .env file
load_dotenv()

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("siorg_load.log", encoding="utf-8")
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

# ClickHouse Table Schemas Column Lists (excluding data_carga which has DEFAULT now())
DISTRIBUICAO_COLUMNS = [
    'nivel_hierarquico', 'cod_orgao_entidade', 'nome_orgao_entidade', 'sigla_orgao_entidade',
    'natureza_juridica', 'subnatureza_juridica', 'poder', 'esfera', 'cod_unidade_pai', 'cod_unidade',
    'nome_unidade', 'sigla_unidade', 'endereco', 'endereco_complemento', 'bairro', 'municipio', 'uf',
    'cep', 'telefone', 'email', 'area_atuacao', 'nivel_normatizacao', 'autonomia_gestao_cargos',
    'tipo_cargo', 'categoria_cargo', 'nivel_cargo', 'quantidade', 'denominacao_cargo',
    'complemento_denominacao', 'mobilidade', 'obriga_distribuicao', 'compoe_estrutura', 'autoridade',
    'regra_autoridade', 'regra_cargo_nome_unidade', 'temporario', 'cod_instancia',
    'ano_referencia', 'mes_referencia'
]

COMPLETA_COLUMNS = [
    'codigoUnidade', 'codigoUnidadePai', 'codigoOrgaoEntidade', 'codigoTipoUnidade', 'nome', 'sigla',
    'codigoEsfera', 'codigoPoder', 'codigoNaturezaJuridica', 'codigoSubNaturezaJuridica', 'nivelNormatizacao',
    'versaoConsulta', 'dataInicialVersaoConsulta', 'dataFinalVersaoConsulta', 'operacao',
    'codigoUnidadePaiAnterior', 'codigoOrgaoEntidadeAnterior', 'regulamentoEspecifico', 'codigoCategoriaUnidade',
    'competencia', 'finalidade', 'missao', 'descricaoAtoNormativo', 'areaAtuacao', 'telefone', 'email', 'site',
    'linhaEndereco', 'bairro', 'cep', 'uf', 'municipio', 'pais', 'horarioFuncionamento',
    'ano_referencia', 'mes_referencia'
]

TEMP_DIR = "temp_load"

def create_log_table_if_not_exists(ch_client):
    """Create execution log table in ClickHouse"""
    logger.info("Ensuring metadata table 'log_cargas' exists...")
    query = """
    CREATE TABLE IF NOT EXISTS log_cargas (
        data_execucao DateTime DEFAULT now(),
        nome_arquivo String,
        tempo_carga Float64,
        total_linhas UInt64,
        linhas_por_segundo Float64
    ) ENGINE = MergeTree()
    ORDER BY data_execucao;
    """
    ch_client.command(query)

def file_already_loaded(ch_client, filename):
    """Check if the CSV file was already loaded successfully in ClickHouse"""
    res = ch_client.query("SELECT count() FROM log_cargas WHERE nome_arquivo = %s", [filename])
    return res.result_set[0][0] > 0

def log_execution(ch_client, filename, duration, rows_count, rows_per_sec):
    """Insert loading metadata into ClickHouse"""
    logger.info(f"Saving load stats for '{filename}' to log_cargas...")
    ch_client.insert(
        "log_cargas",
        [[filename, duration, rows_count, rows_per_sec]],
        column_names=["nome_arquivo", "tempo_carga", "total_linhas", "linhas_por_segundo"]
    )

def load_csv_file(ch_client, s3_client, s3_key, ch_table, columns_list):
    """Download a CSV file from S3, parse its contents, and insert it into ClickHouse"""
    filename = os.path.basename(s3_key)
    
    # Extract reference year and month from filename (e.g. "distribuicao-orgaos-siorg-2026-07.csv")
    match = re.search(r'-(\d{4})-(\d{2})\.csv', filename)
    if not match:
        logger.warning(f"Skipping file '{filename}'. Could not parse YYYY-MM snapshot date from filename.")
        return
        
    year = int(match.group(1))
    month = int(match.group(2))
    
    if file_already_loaded(ch_client, filename):
        logger.info(f"File '{filename}' has already been loaded previously. Skipping.")
        return
        
    logger.info(f"Starting load for '{filename}' (Snapshot Reference Date: {year:04d}-{month:02d})")
    
    local_path = os.path.join(TEMP_DIR, filename)
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    start_time = time.time()
    
    # 1. Download file from S3
    logger.info(f"Downloading '{s3_key}' from S3...")
    s3_client.download_file(bucket_name, s3_key, local_path)
    
    # 2. Parse CSV and insert into ClickHouse in batches
    batch_size = 20000
    batch = []
    rows_count = 0
    
    logger.info(f"Reading and parsing local CSV data...")
    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        header = next(reader) # Skip header row
        
        expected_cols_count = len(columns_list) - 2 # Exclude year and month added by python
        
        for row in reader:
            if not row:
                continue
                
            # Basic validation
            if len(row) != expected_cols_count:
                logger.warning(f"Row {rows_count + 1} has invalid column count. Expected {expected_cols_count}, got {len(row)}. Skipping row.")
                continue
                
            # Convert empty strings to None (so they become NULL in ClickHouse Nullable fields)
            processed_row = [val if val != "" else None for val in row]
            
            # Append year and month dimensions
            processed_row.append(year)
            processed_row.append(month)
            
            batch.append(processed_row)
            rows_count += 1
            
            if len(batch) >= batch_size:
                logger.info(f"Inserting batch of {len(batch)} rows into '{ch_table}'...")
                ch_client.insert(ch_table, batch, column_names=columns_list)
                batch = []
                
        # Insert remaining rows
        if batch:
            logger.info(f"Inserting final batch of {len(batch)} rows into '{ch_table}'...")
            ch_client.insert(ch_table, batch, column_names=columns_list)
            
    end_time = time.time()
    duration = end_time - start_time
    rows_per_sec = rows_count / duration if duration > 0 else 0
    
    logger.info(f"SUCCESS: Loaded {rows_count} rows from '{filename}' into '{ch_table}' in {duration:.2f}s ({rows_per_sec:.2f} rows/sec)")
    
    # 3. Log metadata in ClickHouse
    log_execution(ch_client, filename, duration, rows_count, rows_per_sec)
    
    # 4. Clean up local download
    if os.path.exists(local_path):
        os.remove(local_path)

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
    
    # Process files
    for key in s3_files:
        filename = os.path.basename(key)
        
        if "estrutura-organizacional-completa" in key:
            ch_table = "estrutura_organizacional_completa"
            columns_list = COMPLETA_COLUMNS
        elif "distribuicao" in key:
            ch_table = "distribuicao_orgaos"
            columns_list = DISTRIBUICAO_COLUMNS
        else:
            logger.info(f"Ignoring file '{filename}' as it does not match known categories.")
            continue
            
        try:
            load_csv_file(ch_client, s3_client, key, ch_table, columns_list)
        except Exception as ex:
            logger.error(f"Error loading '{filename}': {ex}")
            
    # Clean up temp folder
    try:
        if os.path.exists(TEMP_DIR):
            os.rmdir(TEMP_DIR)
    except Exception:
        pass
        
    logger.info("Execution complete.")
    ch_client.close()

if __name__ == "__main__":
    main()
