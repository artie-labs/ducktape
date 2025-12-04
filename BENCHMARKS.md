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
2025/12/03 17:24:59 Appended 1000000 rows (1 workers) in 10.035456583s
2025/12/03 17:24:59 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/03 17:24:59 Throughput: 111670831.39 bytes/sec (106.50 MiB/sec)
2025/12/03 17:24:59 Throughput: 99646.69 rows/sec
2025/12/03 17:24:59 Worker 0: 111670876.82 bytes/sec (106.50 MiB/sec), 99646.73 rows/sec, elapsed 10.0354525 seconds
```

</details>

<details>
<summary><strong>1KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/03 17:25:55 Appended 1000000 rows (10 workers) in 1.378996167s
2025/12/03 17:25:55 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/03 17:25:55 Throughput: 812669249.43 bytes/sec (775.02 MiB/sec)
2025/12/03 17:25:55 Throughput: 725165.18 rows/sec
2025/12/03 17:25:55 Worker 0: 98700252.66 bytes/sec (94.13 MiB/sec), 88230.17 rows/sec, elapsed 1.133399125 seconds
2025/12/03 17:25:55 Worker 1: 81283277.81 bytes/sec (77.52 MiB/sec), 72516.73 rows/sec, elapsed 1.378992125 seconds
2025/12/03 17:25:55 Worker 2: 96802825.46 bytes/sec (92.32 MiB/sec), 86362.47 rows/sec, elapsed 1.157910417 seconds
2025/12/03 17:25:55 Worker 3: 97086761.23 bytes/sec (92.59 MiB/sec), 86615.78 rows/sec, elapsed 1.154524042 seconds
2025/12/03 17:25:55 Worker 4: 96497680.69 bytes/sec (92.03 MiB/sec), 86090.23 rows/sec, elapsed 1.161571959 seconds
2025/12/03 17:25:55 Worker 5: 100773972.41 bytes/sec (96.11 MiB/sec), 89905.32 rows/sec, elapsed 1.11228125 seconds
2025/12/03 17:25:55 Worker 6: 97410528.10 bytes/sec (92.90 MiB/sec), 86904.63 rows/sec, elapsed 1.150686709 seconds
2025/12/03 17:25:55 Worker 7: 95848351.43 bytes/sec (91.41 MiB/sec), 85510.93 rows/sec, elapsed 1.169441084 seconds
2025/12/03 17:25:55 Worker 8: 100897075.90 bytes/sec (96.22 MiB/sec), 90015.15 rows/sec, elapsed 1.110924167 seconds
2025/12/03 17:25:55 Worker 9: 96988699.93 bytes/sec (92.50 MiB/sec), 86528.29 rows/sec, elapsed 1.155691334 seconds
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
2025/12/03 17:32:22 Appended 1000000 rows (10 workers) in 3m59.980814125s
2025/12/03 17:32:22 Total bytes written: 65632667780 (62592.19 MiB)
2025/12/03 17:32:22 Throughput: 273491312.29 bytes/sec (260.82 MiB/sec)
2025/12/03 17:32:22 Throughput: 4167.00 rows/sec
2025/12/03 17:32:22 Worker 0: 63156068.77 bytes/sec (60.23 MiB/sec), 962.30 rows/sec, elapsed 103.918228417 seconds
2025/12/03 17:32:22 Worker 1: 27783629.84 bytes/sec (26.50 MiB/sec), 423.32 rows/sec, elapsed 236.22863675 seconds
2025/12/03 17:32:22 Worker 2: 63031267.00 bytes/sec (60.11 MiB/sec), 960.36 rows/sec, elapsed 104.127511834 seconds
2025/12/03 17:32:22 Worker 3: 27352867.24 bytes/sec (26.09 MiB/sec), 416.76 rows/sec, elapsed 239.948848584 seconds
2025/12/03 17:32:22 Worker 4: 27776571.26 bytes/sec (26.49 MiB/sec), 423.21 rows/sec, elapsed 236.288667084 seconds
2025/12/03 17:32:22 Worker 5: 63008215.37 bytes/sec (60.09 MiB/sec), 960.01 rows/sec, elapsed 104.165607 seconds
2025/12/03 17:32:22 Worker 6: 63093893.89 bytes/sec (60.17 MiB/sec), 961.32 rows/sec, elapsed 104.024155042 seconds
2025/12/03 17:32:22 Worker 7: 27349224.53 bytes/sec (26.08 MiB/sec), 416.70 rows/sec, elapsed 239.980807959 seconds
2025/12/03 17:32:22 Worker 8: 63057055.40 bytes/sec (60.14 MiB/sec), 960.75 rows/sec, elapsed 104.084926875 seconds
2025/12/03 17:32:22 Worker 9: 27780470.69 bytes/sec (26.49 MiB/sec), 423.27 rows/sec, elapsed 236.255500209 seconds
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
2025/12/03 17:08:05 Appended 1000000 rows (1 workers) in 8.873319125s
2025/12/03 17:08:06 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/03 17:08:06 Throughput: 126296345.73 bytes/sec (120.45 MiB/sec)
2025/12/03 17:08:06 Throughput: 112697.40 rows/sec
2025/12/03 17:08:06 Worker 0: 126296422.24 bytes/sec (120.45 MiB/sec), 112697.47 rows/sec, elapsed 8.87331375 seconds
```

</details>

<details>
<summary><strong>1KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/03 17:09:37 Appended 1000000 rows (10 workers) in 1.559228958s
2025/12/03 17:09:37 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/03 17:09:37 Throughput: 718732020.88 bytes/sec (685.44 MiB/sec)
2025/12/03 17:09:37 Throughput: 641342.63 rows/sec
2025/12/03 17:09:37 Worker 0: 91152122.23 bytes/sec (86.93 MiB/sec), 81482.74 rows/sec, elapsed 1.227253708 seconds
2025/12/03 17:09:37 Worker 1: 87338071.17 bytes/sec (83.29 MiB/sec), 77918.50 rows/sec, elapsed 1.283392208 seconds
2025/12/03 17:09:37 Worker 2: 89799421.65 bytes/sec (85.64 MiB/sec), 80114.39 rows/sec, elapsed 1.248215166 seconds
2025/12/03 17:09:37 Worker 3: 88451122.90 bytes/sec (84.35 MiB/sec), 78911.51 rows/sec, elapsed 1.26724225 seconds
2025/12/03 17:09:37 Worker 4: 71887561.57 bytes/sec (68.56 MiB/sec), 64134.36 rows/sec, elapsed 1.559226625 seconds
2025/12/03 17:09:37 Worker 5: 87831510.48 bytes/sec (83.76 MiB/sec), 78358.72 rows/sec, elapsed 1.2761820830000001 seconds
2025/12/03 17:09:37 Worker 6: 87147216.18 bytes/sec (83.11 MiB/sec), 77748.23 rows/sec, elapsed 1.286202875 seconds
2025/12/03 17:09:37 Worker 7: 87247304.12 bytes/sec (83.21 MiB/sec), 77837.53 rows/sec, elapsed 1.2847273750000001 seconds
2025/12/03 17:09:37 Worker 8: 87639971.33 bytes/sec (83.58 MiB/sec), 78187.84 rows/sec, elapsed 1.278971208 seconds
2025/12/03 17:09:37 Worker 9: 87066850.58 bytes/sec (83.03 MiB/sec), 77676.53 rows/sec, elapsed 1.287390083 seconds
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
2025/12/02 14:57:36 Appended 1000000 rows (1 workers) in 22m10.35203625s
2025/12/02 14:57:36 Total bytes written: 65632667780 (62592.19 MiB)
2025/12/02 14:57:36 Throughput: 49334812.13 bytes/sec (47.05 MiB/sec)
2025/12/02 14:57:36 Throughput: 751.68 rows/sec
2025/12/02 14:57:36 Worker 0: 49334814.14 bytes/sec (47.05 MiB/sec), 751.68 rows/sec, elapsed 1330.351982125 seconds
```

</details>

<details>
<summary><strong>64KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/03 17:40:04 Appended 1000000 rows (10 workers) in 4m4.700770416s
2025/12/03 17:40:04 Total bytes written: 65632667780 (62592.19 MiB)
2025/12/03 17:40:04 Throughput: 268216024.28 bytes/sec (255.79 MiB/sec)
2025/12/03 17:40:04 Throughput: 4086.62 rows/sec
2025/12/03 17:40:04 Worker 0: 26820786.08 bytes/sec (25.58 MiB/sec), 408.66 rows/sec, elapsed 244.700761625 seconds
2025/12/03 17:40:04 Worker 1: 27101093.17 bytes/sec (25.85 MiB/sec), 412.92 rows/sec, elapsed 242.178016916 seconds
2025/12/03 17:40:04 Worker 2: 58941795.78 bytes/sec (56.21 MiB/sec), 898.05 rows/sec, elapsed 111.352036583 seconds
2025/12/03 17:40:04 Worker 3: 58924506.90 bytes/sec (56.19 MiB/sec), 897.79 rows/sec, elapsed 111.384708083 seconds
2025/12/03 17:40:04 Worker 4: 27106495.02 bytes/sec (25.85 MiB/sec), 413.00 rows/sec, elapsed 242.129755083 seconds
2025/12/03 17:40:04 Worker 5: 59212763.13 bytes/sec (56.47 MiB/sec), 902.18 rows/sec, elapsed 110.842471333 seconds
2025/12/03 17:40:04 Worker 6: 58930628.76 bytes/sec (56.20 MiB/sec), 897.88 rows/sec, elapsed 111.373137166 seconds
2025/12/03 17:40:04 Worker 7: 58979748.63 bytes/sec (56.25 MiB/sec), 898.63 rows/sec, elapsed 111.280382708 seconds
2025/12/03 17:40:04 Worker 8: 58892167.01 bytes/sec (56.16 MiB/sec), 897.30 rows/sec, elapsed 111.445873583 seconds
2025/12/03 17:40:04 Worker 9: 59245932.95 bytes/sec (56.50 MiB/sec), 902.69 rows/sec, elapsed 110.780414333 seconds
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
- Truncate `benchmark.main.benchmark_append` after every run.

<details>
<summary><strong>1KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
(No results yet)
```

</details>

<details>
<summary><strong>1KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
(No results yet)
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
- Truncate `benchmark.main.benchmark_append` after every run.
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
