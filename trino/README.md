# Trino + Iceberg + Glue + S3

Este diretorio contem a configuracao local do catalogo `iceberg` usada pelo
container Trino. O catalogo usa AWS Glue como metastore e S3 como storage
Iceberg.

Subir o Trino:

```bash
docker compose up -d
```

Testar:

```bash
python scripts/trino/test_trino_connection.py
```


