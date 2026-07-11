import os
import sys
import clickhouse_connect
from dotenv import load_dotenv

# Load configuration from .env file
load_dotenv()

# Retrieve ClickHouse settings
host = os.getenv("CLICKHOUSE_HOST")
port_str = os.getenv("CLICKHOUSE_PORT", "8443")
username = os.getenv("CLICKHOUSE_USERNAME", "default")
password = os.getenv("CLICKHOUSE_PASSWORD")
database = os.getenv("CLICKHOUSE_DATABASE", "default")

# Check if environment variables are populated
if not host or "your-clickhouse-host" in host:
    print("Error: CLICKHOUSE_HOST is not configured in the .env file.")
    sys.exit(1)

if not password or "your-clickhouse-password" in password:
    print("Error: CLICKHOUSE_PASSWORD is not configured in the .env file.")
    sys.exit(1)

try:
    port = int(port_str)
except ValueError:
    print(f"Error: Invalid CLICKHOUSE_PORT '{port_str}' configured in the .env file. Must be an integer.")
    sys.exit(1)

# SQL Statements for table creation
CREATE_DISTRIBUICAO_TABLE = """
CREATE TABLE IF NOT EXISTS distribuicao_orgaos (
    nivel_hierarquico Nullable(String),
    cod_orgao_entidade Nullable(String),
    nome_orgao_entidade Nullable(String),
    sigla_orgao_entidade Nullable(String),
    natureza_juridica Nullable(String),
    subnatureza_juridica Nullable(String),
    poder Nullable(String),
    esfera Nullable(String),
    cod_unidade_pai Nullable(String),
    cod_unidade Nullable(String),
    nome_unidade Nullable(String),
    sigla_unidade Nullable(String),
    endereco Nullable(String),
    endereco_complemento Nullable(String),
    bairro Nullable(String),
    municipio Nullable(String),
    uf Nullable(String),
    cep Nullable(String),
    telefone Nullable(String),
    email Nullable(String),
    area_atuacao Nullable(String),
    nivel_normatizacao Nullable(String),
    autonomia_gestao_cargos Nullable(String),
    tipo_cargo Nullable(String),
    categoria_cargo Nullable(String),
    nivel_cargo Nullable(String),
    quantidade Nullable(String),
    denominacao_cargo Nullable(String),
    complemento_denominacao Nullable(String),
    mobilidade Nullable(String),
    obriga_distribuicao Nullable(String),
    compoe_estrutura Nullable(String),
    autoridade Nullable(String),
    regra_autoridade Nullable(String),
    regra_cargo_nome_unidade Nullable(String),
    temporario Nullable(String),
    cod_instancia Nullable(String),
    
    -- DWH Snapshot and audit columns
    ano_referencia UInt16,
    mes_referencia UInt8,
    data_carga DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ano_referencia, mes_referencia);
"""

CREATE_COMPLETA_TABLE = """
CREATE TABLE IF NOT EXISTS estrutura_organizacional_completa (
    codigoUnidade Nullable(String),
    codigoUnidadePai Nullable(String),
    codigoOrgaoEntidade Nullable(String),
    codigoTipoUnidade Nullable(String),
    nome Nullable(String),
    sigla Nullable(String),
    codigoEsfera Nullable(String),
    codigoPoder Nullable(String),
    codigoNaturezaJuridica Nullable(String),
    codigoSubNaturezaJuridica Nullable(String),
    nivelNormatizacao Nullable(String),
    versaoConsulta Nullable(String),
    dataInicialVersaoConsulta Nullable(String),
    dataFinalVersaoConsulta Nullable(String),
    operacao Nullable(String),
    codigoUnidadePaiAnterior Nullable(String),
    codigoOrgaoEntidadeAnterior Nullable(String),
    regulamentoEspecifico Nullable(String),
    codigoCategoriaUnidade Nullable(String),
    competencia Nullable(String),
    finalidade Nullable(String),
    missao Nullable(String),
    descricaoAtoNormativo Nullable(String),
    areaAtuacao Nullable(String),
    telefone Nullable(String),
    email Nullable(String),
    site Nullable(String),
    linhaEndereco Nullable(String),
    bairro Nullable(String),
    cep Nullable(String),
    uf Nullable(String),
    municipio Nullable(String),
    pais Nullable(String),
    horarioFuncionamento Nullable(String),
    
    -- DWH Snapshot and audit columns
    ano_referencia UInt16,
    mes_referencia UInt8,
    data_carga DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ano_referencia, mes_referencia);
"""

print(f"Connecting to ClickHouse Cloud at {host}:{port}...")

try:
    # 1. First connect without specifying database to ensure target database exists
    print(f"Ensuring database '{database}' exists...")
    temp_client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        secure=True
    )
    temp_client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    temp_client.close()

    # 2. Connect directly to target database
    print(f"Connecting to database '{database}'...")
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        secure=True
    )
    
    # 3. Drop existing tables if they exist to force clean recreation
    print("Dropping existing table 'distribuicao_orgaos' if it exists...")
    client.command("DROP TABLE IF EXISTS distribuicao_orgaos")
    
    print("Dropping existing table 'estrutura_organizacional_completa' if it exists...")
    client.command("DROP TABLE IF EXISTS estrutura_organizacional_completa")
    
    # 4. Create first table
    print("Creating table 'distribuicao_orgaos'...")
    client.command(CREATE_DISTRIBUICAO_TABLE)
    print("Table 'distribuicao_orgaos' created successfully.")
    
    # 5. Create second table
    print("Creating table 'estrutura_organizacional_completa'...")
    client.command(CREATE_COMPLETA_TABLE)
    print("Table 'estrutura_organizacional_completa' created successfully.")
    
    print("\nAll database structures created successfully!")
    client.close()
    
except Exception as e:
    print(f"\nFailed to create tables in ClickHouse Cloud: {e}")
    sys.exit(1)
