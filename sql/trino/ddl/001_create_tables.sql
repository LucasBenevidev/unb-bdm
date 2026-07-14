CREATE SCHEMA IF NOT EXISTS iceberg.siorg
WITH (location = 's3://unb-bdm-siorg/iceberg/siorg/');

-- As tabelas finais sao criadas por create_iceberg_tables.py a partir dos
-- cabecalhos reais do S3. Todas as colunas de origem ficam como VARCHAR para
-- preservar o CSV SIORG e permitir casts controlados nas consultas.

