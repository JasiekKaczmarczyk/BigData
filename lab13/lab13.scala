// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1

// COMMAND ----------

import agh.wggios.analizadanych._

// COMMAND ----------

val params: Array[String] = Array("dbfs:/FileStore/tables/2010_summary.csv")

// COMMAND ----------

Main.main(params)

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2

// COMMAND ----------

// MAGIC %md
// MAGIC W klastrze, w zakładce Metrics, pod live matrics, znajduje się Ganglia UI.

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 3

// COMMAND ----------

// MAGIC %md
// MAGIC Można w ustawieniach klastra zmniejszyć drastycznie ilość pamięci przydzielonej np. spark.executor.memory 1b lub spark.driver.memory 1b

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 4

// COMMAND ----------

import org.apache.spark.sql.functions._

val path = "/FileStore/tables/online_retail_dataset.csv"
var df = spark.read
            .format("csv")
            .option("header","true")
            .load(path)



val df_count = df.groupBy("CustomerID").count()

// COMMAND ----------

df_count.write
  .format("parquet")
  .mode("overwrite")
  .bucketBy(10,"count")
  .saveAsTable("bucketedFiles")

// COMMAND ----------

df_count.write
  .format("parquet")
  .mode("overwrite")
  .partitionBy("count")
  .saveAsTable("partionFiles")

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 5

// COMMAND ----------

// MAGIC %sql
// MAGIC ANALYZE TABLE bucketedFiles COMPUTE STATISTICS NOSCAN;

// COMMAND ----------

// MAGIC %sql
// MAGIC DESC EXTENDED bucketedFiles
