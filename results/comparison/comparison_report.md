# Comparacao ClickHouse vs Trino/Iceberg

## Ingestao

| engine | execucoes | duracao_media_s | duracao_min_s | duracao_max_s | throughput_medio_lps | linhas |
| --- | --- | --- | --- | --- | --- | --- |
| clickhouse | 11 | 87.878461 | 44.171542 | 212.285785 | 158111.697073 | 9375146.000000 |
| trino | 1 | 407.075984 | 407.075984 | 407.075984 | 23030.457135 | 9375146.000000 |

## Consultas

| engine | usuarios | modo | query_id | count | mean | p50 | p95 | p99 | result_rows_median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| clickhouse | 1 | sequential | q01 | 11 | 0.519424 | 0.487270 | 0.868154 | 1.007239 | 46.000000 |
| clickhouse | 5 | mixed-workload | q01 | 55 | 0.948905 | 0.905947 | 1.596228 | 2.022608 | 46.000000 |
| clickhouse | 10 | mixed-workload | q01 | 110 | 1.753089 | 1.508729 | 3.197441 | 4.306076 | 46.000000 |
| clickhouse | 20 | mixed-workload | q01 | 220 | 3.595598 | 2.965862 | 7.022930 | 7.149364 | 46.000000 |
| trino | 1 | mixed-workload | q01 | 1 | 2.840863 | 2.840863 | 2.840863 | 2.840863 | 46.000000 |
| trino | 5 | mixed-workload | q01 | 5 | 6.239361 | 6.441195 | 6.729092 | 6.737864 | 46.000000 |
| trino | 10 | mixed-workload | q01 | 10 | 12.775040 | 12.718431 | 13.656472 | 13.677392 | 46.000000 |
| trino | 20 | mixed-workload | q01 | 20 | 27.012845 | 26.803021 | 29.808160 | 29.842554 | 46.000000 |
| clickhouse | 1 | sequential | q02 | 11 | 1.881954 | 1.578645 | 2.690586 | 2.695209 | 9849.000000 |
| clickhouse | 5 | mixed-workload | q02 | 55 | 4.872002 | 4.270279 | 8.106076 | 9.467466 | 9849.000000 |
| clickhouse | 10 | mixed-workload | q02 | 110 | 8.554132 | 7.180405 | 16.515720 | 21.362220 | 9849.000000 |
| clickhouse | 20 | mixed-workload | q02 | 220 | 18.184869 | 15.259176 | 37.068893 | 38.693710 | 9849.000000 |
| trino | 1 | mixed-workload | q02 | 1 | 3.264267 | 3.264267 | 3.264267 | 3.264267 | 9849.000000 |
| trino | 5 | mixed-workload | q02 | 5 | 8.646700 | 8.737190 | 9.128096 | 9.166898 | 9849.000000 |
| trino | 10 | mixed-workload | q02 | 10 | 19.794160 | 19.816783 | 20.527578 | 20.564414 | 9849.000000 |
| trino | 20 | mixed-workload | q02 | 20 | 44.136848 | 44.004914 | 45.536210 | 45.781276 | 9849.000000 |
| clickhouse | 1 | sequential | q03 | 11 | 0.455234 | 0.410919 | 0.648989 | 0.673228 | 13927.000000 |
| clickhouse | 5 | mixed-workload | q03 | 55 | 0.870766 | 0.758178 | 1.569079 | 1.820311 | 13927.000000 |
| clickhouse | 10 | mixed-workload | q03 | 110 | 1.522090 | 1.177806 | 3.573549 | 4.246071 | 13927.000000 |
| clickhouse | 20 | mixed-workload | q03 | 220 | 3.478813 | 2.595728 | 6.823325 | 7.286635 | 13927.000000 |
| trino | 1 | mixed-workload | q03 | 1 | 1.513702 | 1.513702 | 1.513702 | 1.513702 | 13927.000000 |
| trino | 5 | mixed-workload | q03 | 5 | 4.836612 | 4.633191 | 5.305150 | 5.306812 | 13927.000000 |
| trino | 10 | mixed-workload | q03 | 10 | 9.465803 | 9.482743 | 9.954823 | 9.995954 | 13927.000000 |
| trino | 20 | mixed-workload | q03 | 20 | 20.835840 | 20.511352 | 22.434029 | 22.450899 | 13927.000000 |

## Armazenamento

| engine | run_id | table | data_files | compressed_bytes | compressed_mib | uncompressed_bytes | uncompressed_mib | compression_ratio | source_csv_bytes | csv_to_engine_ratio | total_rows |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| clickhouse |  | estrutura_organizacional_completa |  | 545028833 | 519.780000 | 6517612871 | 6215.679999 | 11.950000 |  |  | 4023834 |
| clickhouse |  | distribuicao_orgaos |  | 139680808 | 133.209999 | 2050846883 | 1955.839999 | 14.700000 |  |  | 5351312 |
| trino | 3 | distribuicao_orgaos | 45 | 173712932 | 165.665562 | 477018790 | 454.920568 | 2.746018 | 1781976087 | 10.258166 |  |
| trino | 3 | estrutura_organizacional_completa | 46 | 537974403 | 513.052371 | 3519245681 | 3356.214219 | 6.541660 | 6334533830 | 11.774787 |  |

## Observacoes

- A ingestao ClickHouse usa `log_cargas.csv` quando disponivel; `log_carga.csv` e usado apenas como fallback legado.
- As consultas sao agrupadas por `concurrency_level` no ClickHouse e por `usuarios` no Trino.
- A comparacao filtra consultas Trino com cardinalidade final equivalente ao ClickHouse.
