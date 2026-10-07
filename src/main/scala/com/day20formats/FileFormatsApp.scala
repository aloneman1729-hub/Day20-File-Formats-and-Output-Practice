package com.day20formats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FileFormatsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day20-File-Formats-and-Output")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("==============================================")
    println("DAY 20 - FILE FORMATS AND OUTPUT")
    println("==============================================")

    // --------------------------------------------------
    // 1. READ CSV
    // --------------------------------------------------

    println("\n1. READING CSV")

    val salesCsv = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sales.csv")

    salesCsv.show(10, truncate = false)

    salesCsv.printSchema()

    println(s"CSV Record Count: ${salesCsv.count()}")

    // --------------------------------------------------
    // 2. READ JSON
    // --------------------------------------------------

    println("\n2. READING JSON")

val salesJson = spark.read
  .option("inferSchema", "true")
  .option("multiLine", "true")
  .json("data/sales.json")
    salesJson.show(10, truncate = false)

    salesJson.printSchema()

    println(s"JSON Record Count: ${salesJson.count()}")

    // --------------------------------------------------
    // 3. PREPARE DATA
    // --------------------------------------------------

    println("\n3. PREPARING SALES DATA")

    val sales = salesCsv
      .withColumn(
        "sale_date",
        to_date(col("sale_date"), "yyyy-MM-dd")
      )
      .withColumn("year", year(col("sale_date")))
      .withColumn("month", month(col("sale_date")))
      .withColumn("day", dayofmonth(col("sale_date")))

    sales.show(10, truncate = false)

    // --------------------------------------------------
    // 4. REPARTITION
    // --------------------------------------------------

    println("\n4. REPARTITIONING DATA")

    println(s"Partitions before repartition: ${sales.rdd.getNumPartitions}")

    val repartitionedSales = sales.repartition(3)

    println(
      s"Partitions after repartition: ${repartitionedSales.rdd.getNumPartitions}"
    )

    // --------------------------------------------------
    // 5. WRITE CSV
    // --------------------------------------------------

    println("\n5. WRITING CSV")

    repartitionedSales
      .drop("year", "month", "day")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/csv")

    println("CSV output written to output/csv")

    // --------------------------------------------------
    // 6. WRITE JSON
    // --------------------------------------------------

    println("\n6. WRITING JSON")

    repartitionedSales
      .write
      .mode("overwrite")
      .json("output/json")

    println("JSON output written to output/json")

    // --------------------------------------------------
    // 7. WRITE PARQUET
    // --------------------------------------------------

    println("\n7. WRITING PARQUET")

    repartitionedSales
      .write
      .mode("overwrite")
      .parquet("output/parquet")

    println("Parquet output written to output/parquet")

    // --------------------------------------------------
    // 8. WRITE PARTITIONED PARQUET
    // --------------------------------------------------

    println("\n8. WRITING PARTITIONED PARQUET")

    repartitionedSales
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/partitioned-sales")

    println("Partitioned Parquet output written.")

    // --------------------------------------------------
    // 9. READ PARTITIONED PARQUET
    // --------------------------------------------------

    println("\n9. READING PARTITIONED PARQUET")

    val partitionedSales = spark.read
      .parquet("output/partitioned-sales")

    partitionedSales.show(20, truncate = false)

    partitionedSales.printSchema()

    // --------------------------------------------------
    // 10. DAILY SALES SUMMARY
    // --------------------------------------------------

    println("\n10. DAILY SALES SUMMARY")

    val dailySales = sales
      .groupBy("year", "month", "day")
      .agg(
        count("*").alias("number_of_sales"),
        round(sum("amount"), 2).alias("total_sales")
      )
      .orderBy("year", "month", "day")

    dailySales.show(truncate = false)

    // --------------------------------------------------
    // 11. FORMAT COMPARISON
    // --------------------------------------------------

    println("\n11. FILE FORMAT SUMMARY")

    println("CSV     -> Human-readable, simple, larger storage")
    println("JSON    -> Semi-structured, flexible schema")
    println("Parquet -> Columnar, compressed, efficient for analytics")

    println("\n==============================================")
    println("DAY 20 PROJECT COMPLETED")
    println("==============================================")

    spark.stop()
  }
}
