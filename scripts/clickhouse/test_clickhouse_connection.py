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

print(f"Attempting to connect to ClickHouse Cloud at {host}:{port} as user '{username}'...")

try:
    # Establish connection
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        secure=True
    )
    
    # 1. Run simple command
    print("Executing SELECT 1...")
    res = client.command("SELECT 1")
    print(f"Connection test result: {res} (Success)")
    
    # 2. Test table creation, insertion and query
    test_table = "test_connection_siorg"
    print(f"Creating temporary test table '{test_table}'...")
    client.command(f"DROP TABLE IF EXISTS {test_table}")
    client.command(f"CREATE TABLE {test_table} (id Int32, message String) ENGINE = MergeTree() ORDER BY id")
    
    print("Inserting test row...")
    client.insert(test_table, [[1, "Hello from SIORG Python Uploader!"]], column_names=["id", "message"])
    
    print("Querying test row...")
    query_res = client.query(f"SELECT * FROM {test_table}")
    print("Query results:")
    for row in query_res.result_set:
        print(f"  ID: {row[0]}, Message: '{row[1]}'")
        
    print(f"Dropping temporary test table '{test_table}'...")
    client.command(f"DROP TABLE {test_table}")
    
    print("\nClickHouse Cloud connection and permissions verified successfully!")
    client.close()
    
except Exception as e:
    print("\nConnection to ClickHouse Cloud failed!")
    print(f"Error details: {e}")
    print("\nPlease verify that:")
    print("1. The host and password in the .env file are correct and contain no extra spaces.")
    print("2. Your internet connection is active and not blocked by a firewall (port 8443 must be open).")
    sys.exit(1)
