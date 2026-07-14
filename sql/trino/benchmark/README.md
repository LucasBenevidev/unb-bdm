# Consultas Trino

As consultas deste diretorio sao traducoes semanticas das tres consultas de
`docs/Consultas.md` e `scripts/clickhouse/benchmark_queries.py`.

- Q01 troca `lagInFrame` por `lag` de Trino e preserva a serie temporal.
- Q02 troca o operador ternario do ClickHouse por `CASE WHEN`.
- Q03 troca `toUInt8OrZero` por `COALESCE(try_cast(... AS integer), 0)`.


