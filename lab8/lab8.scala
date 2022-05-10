// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

// MAGIC %md **<h4> Repartition </h4>**

// COMMAND ----------

import org.apache.spark.sql.functions.spark_partition_id

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(2000)
.groupBy("job").sum()


df.explain
print(df.count())
df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(1)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(7)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(9)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(16)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(24)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(96)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(200)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(4000)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md **Coalesce**

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(6)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(5)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(4)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(3)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(2)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actors_par.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(1)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md **Poka Yoke**

// COMMAND ----------

//poka yoke
import org.apache.spark.sql.functions.udf

// COMMAND ----------

val mean = (s: List[Integer]) =>
{
 if (s.count == 0) 
    0
 else s.sum/s.count
}
spark.udf.register("mean", mean)

// COMMAND ----------

def Count_Null_in_Column(colName : String, df :DataFrame) : Either[String,Long] ={
  if(df.columns.contains(colName)){
     val count =df.filter(col(colName).isNull).count()
     Right(count)
  }else{
    Left("No such column in data")
  }
}

spark.udf.register("Count_Null_in_Column", Count_Null_in_Column)

// COMMAND ----------

val sqrt_double = (a: Double ) =>
{
  import scala.math.sqrt
 if (a<0) 0 else sqrt(a)
}
spark.udf.register("sqrt_double", sqrt_double)

// COMMAND ----------

def fill_nan(colname: String, df :DataFrame): DataFrame = {
   if(df.columns.contains(colname)){
     return df.na.fill(0,Array(colname))
   }
  else
  {
      Logger.getLogger("Such a column does not exist in df").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }

}

spark.udf.register("fill_nan", fill_nan)

// COMMAND ----------

val PascalCase = (s: String) =>
{
  if(s.count)
   s.split("_").map(_.capitalize).mkString("")
  else
  ""
}

spark.udf.register("PascalCase", PascalCase)
