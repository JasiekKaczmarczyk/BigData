 
// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1

// COMMAND ----------

val data_tran = Seq(( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500))


val data_log = Seq((1,"George", 800),
(2,"Sam", 950),
(3,"Diane", 1100),
(4,"Nicholas", 1250),
(5,"Samuel", 1250),
(6,"Patricia", 1300),
(7,"Brian", 1500),
(8,"Thomas", 1600),
(9,"Fran", 2450),
(10,"Debbie", 2850),
(11,"Mark", 2975),
(12,"James", 3000),
(13,"Cynthia", 3000),
(14,"Christopher", 5000))

// COMMAND ----------

import org.apache.spark.sql.functions._
val col_names_tran = Seq("AccountId", "TranDate", "TranAmt")
var df_tran = spark.createDataFrame(data_tran).toDF(col_names_tran:_*)
df_tran = df_tran.withColumn("TranDate",to_date($"TranDate","yyyy-MM-dd"))
display(df_tran)

// COMMAND ----------

val col_names_log = Seq("RowID","FName", "Salary")
var df_log = spark.createDataFrame(data_log).toDF(col_names_log:_*)
display(df_log)

// COMMAND ----------

// Totals based on previous row
import org.apache.spark.sql.expressions.Window
val windowSpec  = Window.partitionBy("AccountId").orderBy("TranDate")
df_tran.withColumn("RunTotalAmtr", sum("TranAmt").over(windowSpec))
       .orderBy("AccountId", "TranDate")
       .show()

// COMMAND ----------

df_tran.withColumn("RunAvg", avg("TranAmt").over(windowSpec))
       .withColumn("RunTranQty", count("*").over(windowSpec))
       .withColumn("RunSmallAmt", min("TranAmt").over(windowSpec))
       .withColumn("RunLargeAmt", max("TranAmt").over(windowSpec))
       .withColumn("RunTotalAmt", sum("TranAmt").over(windowSpec))
       .orderBy("AccountId", "TranDate")
       .show()

// COMMAND ----------

// Calculating Totals Based Upon a Subset of Rows
val windowSpec2  = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)
df_tran.withColumn("SlideAvg", avg("TranAmt").over(windowSpec2))
       .withColumn("SlideQty", count("*").over(windowSpec2))
       .withColumn("SlideMin", min("TranAmt").over(windowSpec2))
       .withColumn("SlideMax", max("TranAmt").over(windowSpec2))
       .withColumn("SlideTotal", sum("TranAmt").over(windowSpec2))
       .withColumn("RN", row_number.over(windowSpec))
       .orderBy("AccountId", "TranDate", "RN")
       .show()

// COMMAND ----------

// Logical Window
val windowSpec3  = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding,Window.currentRow)
val windowSpec4  = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding,Window.currentRow)
df_log.withColumn("SumByRows", sum("Salary").over(windowSpec3))
      .withColumn("SumByRange", sum("Salary").over(windowSpec4))
      .orderBy("RowID")
      .show()

// COMMAND ----------

val df_sales = spark.read.format("delta").load(s"dbfs:/FileStore/tables/Files/SalesLt/SalesOrderHeader")
display(df_sales)

// COMMAND ----------

val windowSpec5  = Window.partitionBy("AccountNumber").orderBy("OrderDate")
df_sales.select($"AccountNumber", $"OrderDate", $"TotalDue", row_number().over(windowSpec5).as("RN"))
      .orderBy("AccountNumber")
      .limit(10)
      .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2

// COMMAND ----------

val windowRows = windowSpec.rowsBetween(Window.unboundedPreceding,-1)
df_tran.withColumn("RunLead", lead("TranAmt",2).over(windowSpec))
       .withColumn("RunLag", lag("TranAmt",2).over(windowSpec))
       .withColumn("RunFirst", first("TranAmt").over(windowRows))
       .withColumn("RunLast", last("TranAmt").over(windowRows))
       .withColumn("RunRow", row_number().over(windowSpec))
       .withColumn("RunDenseRank", dense_rank().over(windowSpec))
       .orderBy("AccountId", "TranDate")
       .show()

// COMMAND ----------

val windowSpec6  = Window.partitionBy("AccountId").orderBy("TranAmt").rangeBetween(Window.unboundedPreceding,-1)
df_tran.withColumn("RunFirst", first("TranAmt").over(windowSpec6))
       .withColumn("RunLast", last("TranAmt").over(windowSpec6))
       .orderBy("AccountId", "TranDate")
       .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 3

// COMMAND ----------

val df_customer = spark.read.format("delta").load(s"dbfs:/FileStore/tables/Files/SalesLt/Customer")
display(df_customer)

// COMMAND ----------

val df_address = spark.read.format("delta").load(s"dbfs:/FileStore/tables/Files/SalesLt/CustomerAddress")
display(df_address)

// COMMAND ----------

df_customer.join(df_address, df_customer("CustomerID") === df_address("CustomerID"),"leftsemi").explain()

// COMMAND ----------

df_customer.join(df_address,df_customer("CustomerID") === df_address("CustomerID"),"leftanti").explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 4

// COMMAND ----------

// usuwanie duplikatów
val join1 = df_customer.join(df_address, "CustomerID")
display(join1)

// COMMAND ----------

val join2 = df_customer.join(df_address, Seq("CustomerID"))
display(join2)

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 5

// COMMAND ----------

df_customer.join(broadcast(df_address),df_customer("CustomerID") === df_address("CustomerID")).explain()
