# Day 20 - File Formats and Output

A Scala + Apache Spark project demonstrating how to read and write CSV, JSON, and Parquet files, control Spark partitions, and write partitioned output for analytics.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.1
- SBT
- Java 17

## Project Structure

```text
Day20-File-Formats-and-Output-Practice/
├── build.sbt
├── .gitignore
├── README.md
├── data/
│   ├── sales.csv
│   └── sales.json
├── output/
└── src/
    └── main/
        └── scala/
            └── com/
                └── day20formats/
                    └── FileFormatsApp.scala
Concepts Covered
1. Reading CSV

The project reads sales data from CSV using Spark DataFrame APIs.

spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/sales.csv")
2. Reading JSON

The project reads multiline JSON data.

spark.read
  .option("inferSchema", "true")
  .option("multiLine", "true")
  .json("data/sales.json")
3. Repartitioning

The sales DataFrame is repartitioned into three Spark partitions:

sales.repartition(3)

This demonstrates how Spark partitions data before writing.

4. Writing CSV
.write
.mode("overwrite")
.option("header", "true")
.csv("output/csv")
5. Writing JSON
.write
.mode("overwrite")
.json("output/json")
6. Writing Parquet
.write
.mode("overwrite")
.parquet("output/parquet")

Parquet is a columnar storage format that is efficient for analytical workloads.

7. Partitioned Parquet

Sales are partitioned by year, month, and day:

.write
.mode("overwrite")
.partitionBy("year", "month", "day")
.parquet("output/partitioned-sales")

The resulting layout follows this pattern:

output/partitioned-sales/
└── year=2026/
    └── month=10/
        ├── day=1/
        ├── day=2/
        ├── day=3/
        ├── day=4/
        └── day=5/
8. Number of Output Files

repartition(3) creates three Spark partitions, but it does not guarantee exactly three physical files inside every partitioned directory.

The number of physical files depends on how records are distributed across Spark partitions and partition columns.

Spark also creates _SUCCESS to indicate successful completion of the write operation.

Daily Sales Summary
Date	Number of Sales	Total Sales
2026-10-01	3	78,700
2026-10-02	3	103,500
2026-10-03	3	36,300
2026-10-04	3	102,700
2026-10-05	3	57,300
Total	15	378,500
File Format Comparison
Format	Characteristics
CSV	Simple and human-readable
JSON	Flexible and semi-structured
Parquet	Columnar, compressed, efficient for analytics
How to Run

Compile the project:

sbt compile

Run the application:

sbt run

Generated output is stored under:

output/

The output/ and target/ directories are excluded from Git using .gitignore.

Learning Outcome

This project demonstrates practical Spark file handling, partitioning, repartitioning, and output management techniques used in data engineering pipelines.
