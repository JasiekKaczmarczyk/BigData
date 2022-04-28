 // Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

//Zadanie 1
//Czy Hive wspiera indeksy? Jeśli staniesz przed problemem – czy da się szybko wyciągać dane?
//Odpowiedz na pytanie i podaj przykłady jak rozwiążesz problem indeksów w Hive.


// COMMAND ----------

// MAGIC %md
// MAGIC Hive ma ograniczone możliwości indeksowania. Nie ma kluczy w zwykłym sensie relacyjnej bazy danych, ale można utworzyć indeks na kolumnach, aby przyspieszyć niektóre operacje. Dane indeksu dla tabeli są przechowywane w innej tabeli.
// MAGIC
// MAGIC Apache Hive wspieral indeksacje pod katem optymalizacji zapytan do wersji 3.0. Od wersji 3.0 zostala ona usunieta (zrodlo: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Indexing).W celu rozwiazania problemu indeksacji mozna rozwazyc wybor np. SQLite3, Hive jest lepszu do danych nieustruktyrozwanych, konfiguracji, pamieci podrecznej itd.Innym rozwiazaniem jest uzycie plikow .ORC, ktore maja wbudowane indeksy, co pozwala pomijac bloki danych w trakcie operacji odczytu.
// MAGIC
// COMMAND ----------

//Zadanie 2
//Stwórz diagram draw.io pokazujący jak powinien wyglądać pipeline. Wymysł kilka transfomracji, np. usunięcie kolumny, wyliczenie współczynnika dla x gdzie wartość do formuły jest w pliku //referencyjnym
//Wymagania:
//Ilość źródeł: 7; 5 rodzajów plików, 2 bazy danych,

// COMMAND ----------

//Zadanie 3
//Napisz funkcję, która usunie danych ze wszystkich tabel w konkretniej bazie danych. Informacje pobierz z obiektu Catalog.

// COMMAND ----------

spark.catalog.listDatabases().show()

// COMMAND ----------

//create database
spark.sql("CREATE DATABASE lab7")

// COMMAND ----------

//create tables:
val filePath = "dbfs:/FileStore/tables/names.csv"
val df = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)

df.write.mode("overwrite").saveAsTable("lab7.names")


// COMMAND ----------

val filePath2 = "dbfs:/FileStore/tables/actors.csv"
val df = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath2)

df.write.mode("overwrite").saveAsTable("lab7.actors")

// COMMAND ----------

spark.catalog.listTables("lab7").show()

// COMMAND ----------

val xx=spark.sql(s"SELECT * FROM lab7.actors")
xx.show()

// COMMAND ----------

//funkcja do usuwania danych:

def drop_data(database: String){

  val tables = spark.catalog.listTables(s"$database")
  val tables_names = tables.select("name").as[String].collect.toList
  var i = List()

  for( i <- tables_names){
    spark.sql(s"DELETE FROM $database.$i")
    print(f"Data from table $i deleted   ")
  }
}


// COMMAND ----------

drop_data("lab7")

// COMMAND ----------

val check=spark.sql(s"SELECT * FROM lab7.actors")
check.show()
