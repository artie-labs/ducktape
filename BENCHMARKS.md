# Scripts
- [In-process Append](cmd/benchinprocess/main.go)
- [Append with ducktape](cmd/bench/main.go)


# In-process Append to local filesystem

This is the baseline performance for the [DuckDB Appender API](https://duckdb.org/docs/stable/data/appender.html).
Run on M2 Max Macbook Pro with 32GB RAM and 1TB SSD.

| Test                               | Command                                                                                        |
| ---------------------------------- | ---------------------------------------------------------------------------------------------- |
| 1KB row size, 1M rows, 1 stream    | `go run cmd/benchinprocess/main.go -dsn 'duckdb:benchmark.db' -concurrency 1`                  |
| 1KB row size, 1M rows, 10 streams  | `go run cmd/benchinprocess/main.go -dsn 'duckdb:benchmark.db' -concurrency 10`                 |
| 64KB row size, 1M rows, 1 stream   | `go run cmd/benchinprocess/main.go -dsn 'duckdb:benchmark.db' -concurrency 1 -row-size 65536`  |
| 64KB row size, 1M rows, 10 streams | `go run cmd/benchinprocess/main.go -dsn 'duckdb:benchmark.db' -concurrency 10 -row-size 65536` |

Notes:
- Delete `benchmark.db` (and `benchmark.db.wal` if it exists) after every run.

<details>
<summary><strong>1KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
2025/12/04 10:25:59 Appended 1000000 rows (1 workers) in 8.539055458s
2025/12/04 10:25:59 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/04 10:25:59 Throughput: 131240250.81 bytes/sec (125.16 MiB/sec)
2025/12/04 10:25:59 Throughput: 117108.97 rows/sec
2025/12/04 10:25:59 Worker 0: 131240295.00 bytes/sec (125.16 MiB/sec), 117109.01 rows/sec, elapsed 8.539052583 seconds
```

</details>

<details>
<summary><strong>1KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/04 10:11:12 Appended 1000000 rows (10 workers) in 1.260621292s
2025/12/04 10:11:12 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/04 10:11:12 Throughput: 888980526.60 bytes/sec (847.80 MiB/sec)
2025/12/04 10:11:12 Throughput: 793259.65 rows/sec
2025/12/04 10:11:12 Worker 0: 108961870.16 bytes/sec (103.91 MiB/sec), 97403.24 rows/sec, elapsed 1.026659875 seconds
2025/12/04 10:11:12 Worker 1: 106577613.98 bytes/sec (101.64 MiB/sec), 95083.03 rows/sec, elapsed 1.051712417 seconds
2025/12/04 10:11:12 Worker 2: 108604869.76 bytes/sec (103.57 MiB/sec), 96891.64 rows/sec, elapsed 1.032080792 seconds
2025/12/04 10:11:12 Worker 3: 88915968.46 bytes/sec (84.80 MiB/sec), 79326.22 rows/sec, elapsed 1.260617209 seconds
2025/12/04 10:11:12 Worker 4: 106331272.30 bytes/sec (101.41 MiB/sec), 94863.25 rows/sec, elapsed 1.054148959 seconds
2025/12/04 10:11:12 Worker 5: 107520004.60 bytes/sec (102.54 MiB/sec), 95923.78 rows/sec, elapsed 1.042494375 seconds
2025/12/04 10:11:12 Worker 6: 106954595.55 bytes/sec (102.00 MiB/sec), 95419.35 rows/sec, elapsed 1.048005459 seconds
2025/12/04 10:11:12 Worker 7: 106451492.97 bytes/sec (101.52 MiB/sec), 94970.51 rows/sec, elapsed 1.052958459 seconds
2025/12/04 10:11:12 Worker 8: 108468515.23 bytes/sec (103.44 MiB/sec), 96769.99 rows/sec, elapsed 1.033378209 seconds
2025/12/04 10:11:12 Worker 9: 107875417.58 bytes/sec (102.88 MiB/sec), 96240.86 rows/sec, elapsed 1.039059709 seconds
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
(No results yet)
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/04 10:16:19 Appended 1000000 rows (10 workers) in 3m54.831375583s
2025/12/04 10:16:19 Total bytes written: 65632667780 (62592.19 MiB)
2025/12/04 10:16:19 Throughput: 279488495.17 bytes/sec (266.54 MiB/sec)
2025/12/04 10:16:19 Throughput: 4258.37 rows/sec
2025/12/04 10:16:19 Worker 0: 69264136.65 bytes/sec (66.06 MiB/sec), 1055.36 rows/sec, elapsed 94.754184458 seconds
2025/12/04 10:16:19 Worker 1: 69389258.12 bytes/sec (66.17 MiB/sec), 1057.23 rows/sec, elapsed 94.586527917 seconds
2025/12/04 10:16:19 Worker 2: 68315668.74 bytes/sec (65.15 MiB/sec), 1040.88 rows/sec, elapsed 96.072967167 seconds
2025/12/04 10:16:19 Worker 3: 28046754.47 bytes/sec (26.75 MiB/sec), 427.33 rows/sec, elapsed 234.012424 seconds
2025/12/04 10:16:19 Worker 4: 68650971.52 bytes/sec (65.47 MiB/sec), 1045.98 rows/sec, elapsed 95.603730792 seconds
2025/12/04 10:16:19 Worker 5: 68537841.48 bytes/sec (65.36 MiB/sec), 1044.26 rows/sec, elapsed 95.761536375 seconds
2025/12/04 10:16:19 Worker 6: 68494170.77 bytes/sec (65.32 MiB/sec), 1043.60 rows/sec, elapsed 95.822592292 seconds
2025/12/04 10:16:19 Worker 7: 27948944.48 bytes/sec (26.65 MiB/sec), 425.84 rows/sec, elapsed 234.83137275 seconds
2025/12/04 10:16:19 Worker 8: 28047087.21 bytes/sec (26.75 MiB/sec), 427.33 rows/sec, elapsed 234.009647833 seconds
2025/12/04 10:16:19 Worker 9: 69550902.34 bytes/sec (66.33 MiB/sec), 1059.70 rows/sec, elapsed 94.366698042 seconds
```

</details>

# Append with HTTP/2 client and ducktape server to local filesystem

Run on M2 Max Macbook Pro with 32GB RAM and 1TB SSD.

| Test                               | Command                                                                               |
| ---------------------------------- | ------------------------------------------------------------------------------------- |
| 1KB row size, 1M rows, 1 stream    | `go run cmd/bench/main.go -dsn 'duckdb:benchmark.db' -concurrency 1`                  |
| 1KB row size, 1M rows, 10 streams  | `go run cmd/bench/main.go -dsn 'duckdb:benchmark.db' -concurrency 10`                 |
| 64KB row size, 1M rows, 1 stream   | `go run cmd/bench/main.go -dsn 'duckdb:benchmark.db' -concurrency 1 -row-size 65536`  |
| 64KB row size, 1M rows, 10 streams | `go run cmd/bench/main.go -dsn 'duckdb:benchmark.db' -concurrency 10 -row-size 65536` |

Notes:
- Run ducktape with `go run cmd/main.go`.
- Kill ducktape and start it again for every run to discard any cached connections.
- Delete `benchmark.db` (and `benchmark.db.wal` if it exists) after every run.

<details>
<summary><strong>1KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
2025/12/04 10:24:12 Appended 1000000 rows (1 workers) in 8.529314208s
2025/12/04 10:24:12 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/04 10:24:12 Throughput: 131390139.07 bytes/sec (125.30 MiB/sec)
2025/12/04 10:24:12 Throughput: 117242.72 rows/sec
2025/12/04 10:24:12 Worker 0: 131390214.17 bytes/sec (125.30 MiB/sec), 117242.79 rows/sec, elapsed 8.529309333 seconds
```

</details>

<details>
<summary><strong>1KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/04 10:23:04 Appended 1000000 rows (10 workers) in 1.412208959s
2025/12/04 10:23:04 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/04 10:23:04 Throughput: 793556628.33 bytes/sec (756.79 MiB/sec)
2025/12/04 10:23:04 Throughput: 708110.51 rows/sec
2025/12/04 10:23:04 Worker 0: 96186483.51 bytes/sec (91.73 MiB/sec), 85983.06 rows/sec, elapsed 1.16301975 seconds
2025/12/04 10:23:04 Worker 1: 79371843.43 bytes/sec (75.69 MiB/sec), 70811.45 rows/sec, elapsed 1.412201042 seconds
2025/12/04 10:23:04 Worker 2: 96055414.22 bytes/sec (91.61 MiB/sec), 85695.67 rows/sec, elapsed 1.166920167 seconds
2025/12/04 10:23:04 Worker 3: 82603790.39 bytes/sec (78.78 MiB/sec), 73694.82 rows/sec, elapsed 1.356947417 seconds
2025/12/04 10:23:04 Worker 4: 96664871.18 bytes/sec (92.19 MiB/sec), 86239.39 rows/sec, elapsed 1.159562917 seconds
2025/12/04 10:23:04 Worker 5: 98879258.40 bytes/sec (94.30 MiB/sec), 88214.95 rows/sec, elapsed 1.1335946670000001 seconds
2025/12/04 10:23:04 Worker 6: 96273302.12 bytes/sec (91.81 MiB/sec), 85890.05 rows/sec, elapsed 1.1642791670000001 seconds
2025/12/04 10:23:04 Worker 7: 98526163.34 bytes/sec (93.96 MiB/sec), 87899.94 rows/sec, elapsed 1.137657209 seconds
2025/12/04 10:23:04 Worker 8: 98652410.14 bytes/sec (94.08 MiB/sec), 88012.57 rows/sec, elapsed 1.136201334 seconds
2025/12/04 10:23:04 Worker 9: 96159027.10 bytes/sec (91.70 MiB/sec), 85788.10 rows/sec, elapsed 1.165662792 seconds
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
(No results yet)
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/04 10:31:29 Appended 1000000 rows (10 workers) in 4m6.25614125s
2025/12/04 10:31:29 Total bytes written: 65632667780 (62592.19 MiB)
2025/12/04 10:31:29 Throughput: 266521953.31 bytes/sec (254.18 MiB/sec)
2025/12/04 10:31:29 Throughput: 4060.81 rows/sec
2025/12/04 10:31:29 Worker 0: 27097564.41 bytes/sec (25.84 MiB/sec), 412.88 rows/sec, elapsed 242.201353667 seconds
2025/12/04 10:31:29 Worker 1: 65470406.12 bytes/sec (62.44 MiB/sec), 997.52 rows/sec, elapsed 100.248179125 seconds
2025/12/04 10:31:29 Worker 2: 64971192.78 bytes/sec (61.96 MiB/sec), 989.92 rows/sec, elapsed 101.018447084 seconds
2025/12/04 10:31:29 Worker 3: 26775230.14 bytes/sec (25.53 MiB/sec), 407.95 rows/sec, elapsed 245.125400042 seconds
2025/12/04 10:31:29 Worker 4: 65003095.94 bytes/sec (61.99 MiB/sec), 990.40 rows/sec, elapsed 100.968867792 seconds
2025/12/04 10:31:29 Worker 5: 65429117.75 bytes/sec (62.40 MiB/sec), 996.90 rows/sec, elapsed 100.311439709 seconds
2025/12/04 10:31:29 Worker 6: 64979580.83 bytes/sec (61.97 MiB/sec), 990.05 rows/sec, elapsed 101.005406875 seconds
2025/12/04 10:31:29 Worker 7: 65144048.35 bytes/sec (62.13 MiB/sec), 992.55 rows/sec, elapsed 100.750401084 seconds
2025/12/04 10:31:29 Worker 8: 26652285.99 bytes/sec (25.42 MiB/sec), 406.08 rows/sec, elapsed 246.256137417 seconds
2025/12/04 10:31:29 Worker 9: 65110885.69 bytes/sec (62.09 MiB/sec), 992.05 rows/sec, elapsed 100.801715875 seconds
```

</details>

# In-process Append to MotherDuck

Run with:
- Read/Write Duckling = `Standard`
- Read Scaling Ducklings = `Standard`
- Read Scaling Ducking Pool Size = `4 Ducklings`

| Test                               | Command                                                                                    |
| ---------------------------------- | ------------------------------------------------------------------------------------------ |
| 1KB row size, 1M rows, 1 stream    | `go run cmd/benchinprocess/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 1`                  |
| 1KB row size, 1M rows, 10 streams  | `go run cmd/benchinprocess/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 10`                 |
| 64KB row size, 1M rows, 1 stream   | `go run cmd/benchinprocess/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 1 -row-size 65536`  |
| 64KB row size, 1M rows, 10 streams | `go run cmd/benchinprocess/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 10 -row-size 65536` |

Notes:
- Truncate the `benchmark.main.benchmark_append` table before every run.

<details>
<summary><strong>1KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
2025/12/03 18:45:26 Appended 1000000 rows (1 workers) in 2m12.871971417s
2025/12/03 18:45:26 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/03 18:45:26 Throughput: 8434192.46 bytes/sec (8.04 MiB/sec)
2025/12/03 18:45:26 Throughput: 7526.04 rows/sec
2025/12/03 18:45:26 Worker 0: 8434192.92 bytes/sec (8.04 MiB/sec), 7526.04 rows/sec, elapsed 132.871964208 seconds
```

</details>

<details>
<summary><strong>1KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/03 18:46:31 Appended 1000000 rows (10 workers) in 30.503455666s
2025/12/03 18:46:31 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/03 18:46:31 Throughput: 36739043.35 bytes/sec (35.04 MiB/sec)
2025/12/03 18:46:31 Throughput: 32783.17 rows/sec
2025/12/03 18:46:31 Worker 0: 4117273.30 bytes/sec (3.93 MiB/sec), 3680.51 rows/sec, elapsed 27.170112833 seconds
2025/12/03 18:46:31 Worker 1: 4183163.83 bytes/sec (3.99 MiB/sec), 3732.00 rows/sec, elapsed 26.795269 seconds
2025/12/03 18:46:31 Worker 2: 3834181.23 bytes/sec (3.66 MiB/sec), 3420.66 rows/sec, elapsed 29.234142416 seconds
2025/12/03 18:46:31 Worker 3: 4126318.12 bytes/sec (3.94 MiB/sec), 3681.29 rows/sec, elapsed 27.164410708 seconds
2025/12/03 18:46:31 Worker 4: 4229301.11 bytes/sec (4.03 MiB/sec), 3773.16 rows/sec, elapsed 26.502960458 seconds
2025/12/03 18:46:31 Worker 5: 3674633.60 bytes/sec (3.50 MiB/sec), 3278.32 rows/sec, elapsed 30.503449375 seconds
2025/12/03 18:46:31 Worker 6: 4535932.49 bytes/sec (4.33 MiB/sec), 4046.72 rows/sec, elapsed 24.711346625 seconds
2025/12/03 18:46:31 Worker 7: 3684132.58 bytes/sec (3.51 MiB/sec), 3286.79 rows/sec, elapsed 30.424800833 seconds
2025/12/03 18:46:31 Worker 8: 4321585.14 bytes/sec (4.12 MiB/sec), 3855.49 rows/sec, elapsed 25.937010708 seconds
2025/12/03 18:46:31 Worker 9: 4546666.20 bytes/sec (4.34 MiB/sec), 4056.30 rows/sec, elapsed 24.653008416 seconds
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
(No results yet)
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
(No results yet)
```

</details>

# Append with HTTP/2 client and ducktape server to MotherDuck

Run with:
- Read/Write Duckling = `Standard`
- Read Scaling Ducklings = `Standard`
- Read Scaling Ducking Pool Size = `4 Ducklings`

| Test                               | Command                                                                                    |
| ---------------------------------- | ------------------------------------------------------------------------------------------ |
| 1KB row size, 1M rows, 1 stream    | `go run cmd/bench/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 1`                  |
| 1KB row size, 1M rows, 10 streams  | `go run cmd/bench/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 10`                 |
| 64KB row size, 1M rows, 1 stream   | `go run cmd/bench/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 1 -row-size 65536`  |
| 64KB row size, 1M rows, 10 streams | `go run cmd/bench/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 10 -row-size 65536` |

Notes:
- Truncate the `benchmark.main.benchmark_append` table before every run.
- Run ducktape with `go run cmd/main.go`.
- Kill ducktape and start it again for every run to discard any cached connections.

<details>
<summary><strong>1KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
2025/12/03 17:47:30 Appended 1000000 rows (1 workers) in 2m5.101165167s
2025/12/03 17:47:30 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/03 17:47:30 Throughput: 8958092.26 bytes/sec (8.54 MiB/sec)
2025/12/03 17:47:30 Throughput: 7993.53 rows/sec
2025/12/03 17:47:30 Worker 0: 8958093.24 bytes/sec (8.54 MiB/sec), 7993.53 rows/sec, elapsed 125.1011515 seconds
```

</details>

<details>
<summary><strong>1KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/03 17:48:57 Appended 1000000 rows (10 workers) in 30.821917541s
2025/12/03 17:48:57 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/03 17:48:57 Throughput: 36359443.84 bytes/sec (34.68 MiB/sec)
2025/12/03 17:48:57 Throughput: 32444.44 rows/sec
2025/12/03 17:48:57 Worker 0: 4015997.21 bytes/sec (3.83 MiB/sec), 3589.98 rows/sec, elapsed 27.85529325 seconds
2025/12/03 17:48:57 Worker 1: 4119807.87 bytes/sec (3.93 MiB/sec), 3675.48 rows/sec, elapsed 27.20733675 seconds
2025/12/03 17:48:57 Worker 2: 3988026.28 bytes/sec (3.80 MiB/sec), 3557.91 rows/sec, elapsed 28.1063845 seconds
2025/12/03 17:48:57 Worker 3: 4140117.05 bytes/sec (3.95 MiB/sec), 3693.60 rows/sec, elapsed 27.07387225 seconds
2025/12/03 17:48:57 Worker 4: 4016884.00 bytes/sec (3.83 MiB/sec), 3583.66 rows/sec, elapsed 27.90446525 seconds
2025/12/03 17:48:57 Worker 5: 4009288.17 bytes/sec (3.82 MiB/sec), 3576.88 rows/sec, elapsed 27.957331875 seconds
2025/12/03 17:48:57 Worker 6: 4031944.53 bytes/sec (3.85 MiB/sec), 3597.09 rows/sec, elapsed 27.800233666 seconds
2025/12/03 17:48:57 Worker 7: 4099182.61 bytes/sec (3.91 MiB/sec), 3657.08 rows/sec, elapsed 27.344231916 seconds
2025/12/03 17:48:57 Worker 8: 3636666.08 bytes/sec (3.47 MiB/sec), 3244.45 rows/sec, elapsed 30.8219115 seconds
2025/12/03 17:48:57 Worker 9: 4044642.65 bytes/sec (3.86 MiB/sec), 3608.42 rows/sec, elapsed 27.712955041 seconds
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
2025/12/03 14:32:30 Appended 1000000 rows (1 workers) in 3h56m59.63633825s
2025/12/03 14:32:30 Total bytes written: 65632667780 (62592.19 MiB)
2025/12/03 14:32:30 Throughput: 4615636.17 bytes/sec (4.40 MiB/sec)
2025/12/03 14:32:30 Throughput: 70.33 rows/sec
2025/12/03 14:32:30 Worker 0: 4615636.19 bytes/sec (4.40 MiB/sec), 70.33 rows/sec, elapsed 14219.636263959 seconds
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/03 18:29:25 Appended 1000000 rows (10 workers) in 39m4.875321334s
2025/12/03 18:29:25 Total bytes written: 65632667780 (62592.19 MiB)
2025/12/03 18:29:25 Throughput: 27989832.63 bytes/sec (26.69 MiB/sec)
2025/12/03 18:29:25 Throughput: 426.46 rows/sec
2025/12/03 18:29:25 Worker 0: 2882844.36 bytes/sec (2.75 MiB/sec), 43.93 rows/sec, elapsed 2276.594213042 seconds
2025/12/03 18:29:25 Worker 1: 2885869.38 bytes/sec (2.75 MiB/sec), 43.97 rows/sec, elapsed 2274.284847834 seconds
2025/12/03 18:29:25 Worker 2: 2806912.66 bytes/sec (2.68 MiB/sec), 42.77 rows/sec, elapsed 2338.259076959 seconds
2025/12/03 18:29:25 Worker 3: 2807148.63 bytes/sec (2.68 MiB/sec), 42.77 rows/sec, elapsed 2338.062522042 seconds
2025/12/03 18:29:25 Worker 4: 2889277.46 bytes/sec (2.76 MiB/sec), 44.02 rows/sec, elapsed 2271.602190875 seconds
2025/12/03 18:29:25 Worker 5: 2891519.83 bytes/sec (2.76 MiB/sec), 44.06 rows/sec, elapsed 2269.840566959 seconds
2025/12/03 18:29:25 Worker 6: 2885607.30 bytes/sec (2.75 MiB/sec), 43.97 rows/sec, elapsed 2274.49140075 seconds
2025/12/03 18:29:25 Worker 7: 2888841.80 bytes/sec (2.76 MiB/sec), 44.02 rows/sec, elapsed 2271.944766084 seconds
2025/12/03 18:29:25 Worker 8: 2798992.77 bytes/sec (2.67 MiB/sec), 42.65 rows/sec, elapsed 2344.875295292 seconds
2025/12/03 18:29:25 Worker 9: 2886023.07 bytes/sec (2.75 MiB/sec), 43.97 rows/sec, elapsed 2274.163729667 seconds
```

</details>
