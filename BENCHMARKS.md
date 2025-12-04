# In-process Append to local filesystem

This is the baseline performance for the [DuckDB Appender API](https://duckdb.org/docs/stable/data/appender.html).
Run on M2 Max Macbook Pro with 32GB RAM and 1TB SSD.

| Test                               | Command                                                                                        |
| ---------------------------------- | ---------------------------------------------------------------------------------------------- |
| 1KB row size, 1M rows, 1 stream    | `go run cmd/benchinprocess/main.go -dsn 'duckdb:benchmark.db' -concurrency 1`                  |
| 1KB row size, 1M rows, 10 streams  | `go run cmd/benchinprocess/main.go -dsn 'duckdb:benchmark.db' -concurrency 10`                 |
| 64KB row size, 1M rows, 1 stream   | `go run cmd/benchinprocess/main.go -dsn 'duckdb:benchmark.db' -concurrency 1 -row-size 65536`  |
| 64KB row size, 1M rows, 10 streams | `go run cmd/benchinprocess/main.go -dsn 'duckdb:benchmark.db' -concurrency 10 -row-size 65536` |

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

# Append with HTTP/2 client and ducktape server to MotherDuck

| Test                               | Command                                                                                    |
| ---------------------------------- | ------------------------------------------------------------------------------------------ |
| 1KB row size, 1M rows, 1 stream    | `go run cmd/bench/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 1`                  |
| 1KB row size, 1M rows, 10 streams  | `go run cmd/bench/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 10`                 |
| 64KB row size, 1M rows, 1 stream   | `go run cmd/bench/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 1 -row-size 65536`  |
| 64KB row size, 1M rows, 10 streams | `go run cmd/bench/main.go -dsn 'md:?motherduck_token=xxx' -concurrency 10 -row-size 65536` |

<details>
<summary><strong>1KB row size, 1M rows, 1 stream</strong> - Results</summary>

```
2025/12/02 15:30:35 Appended 1000000 rows (1 workers) in 1m54.4084205s
2025/12/02 15:30:35 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/02 15:30:35 Throughput: 9795326.04 bytes/sec (9.34 MiB/sec)
2025/12/02 15:30:35 Throughput: 8740.62 rows/sec
2025/12/02 15:30:35 Worker 0: 9795326.60 bytes/sec (9.34 MiB/sec), 8740.62 rows/sec, elapsed 114.408413916 seconds
```

</details>

<details>
<summary><strong>1KB row size, 1M rows, 10 streams</strong> - Results</summary>

```
2025/12/02 15:27:36 Appended 1000000 rows (10 workers) in 1m5.160183209s
2025/12/02 15:27:36 Total bytes written: 1120667780 (1068.75 MiB)
2025/12/02 15:27:36 Throughput: 17198659.13 bytes/sec (16.40 MiB/sec)
2025/12/02 15:27:36 Throughput: 15346.80 rows/sec
2025/12/02 15:27:36 Worker 0: 1730460.11 bytes/sec (1.65 MiB/sec), 1546.89 rows/sec, elapsed 64.645685417 seconds
2025/12/02 15:27:36 Worker 1: 1724384.97 bytes/sec (1.64 MiB/sec), 1538.41 rows/sec, elapsed 65.002306459 seconds
2025/12/02 15:27:36 Worker 2: 1730846.94 bytes/sec (1.65 MiB/sec), 1544.17 rows/sec, elapsed 64.759625667 seconds
2025/12/02 15:27:36 Worker 3: 1726007.46 bytes/sec (1.65 MiB/sec), 1539.85 rows/sec, elapsed 64.941202459 seconds
2025/12/02 15:27:36 Worker 4: 1732243.15 bytes/sec (1.65 MiB/sec), 1545.42 rows/sec, elapsed 64.707428667 seconds
2025/12/02 15:27:36 Worker 5: 1720207.09 bytes/sec (1.64 MiB/sec), 1534.68 rows/sec, elapsed 65.160177792 seconds
2025/12/02 15:27:36 Worker 6: 1727628.10 bytes/sec (1.65 MiB/sec), 1541.30 rows/sec, elapsed 64.880282792 seconds
2025/12/02 15:27:36 Worker 7: 1729271.30 bytes/sec (1.65 MiB/sec), 1542.77 rows/sec, elapsed 64.818631917 seconds
2025/12/02 15:27:36 Worker 8: 1723136.54 bytes/sec (1.64 MiB/sec), 1537.29 rows/sec, elapsed 65.049401167 seconds
2025/12/02 15:27:36 Worker 9: 1764830.55 bytes/sec (1.68 MiB/sec), 1574.49 rows/sec, elapsed 63.51261325 seconds
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
(No results yet)
```

</details>
