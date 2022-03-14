// Databricks notebook source
// DBTITLE 1,Wczytanie danych ze schematem
// Wybierz jeden z plik�w csv z poprzednich �wicze� i stw�rz r�cznie schemat danych. Stw�rz DataFrame wczytuj�c plik z u�yciem schematu.
//wczytanie actors.csv i podglad zawartosci tabeli
val filePath = "dbfs:/FileStore/tables/actors.csv"
val actorsDf = spark.read.format("csv")
            .option("header","true")
            .option("inferSchema","true")
            .load(filePath)
display(actorsDf)

import org.apache.spark.sql.types._

val schemat = StructType(Array(
  StructField("imdb_title_id", StringType, true),
  StructField("ordering", IntegerType, true),
  StructField("imdb_name_id", StringType, true),
  StructField("category", StringType, true),
  StructField("job", StringType, true),
  StructField("characters", StringType, true))
)

val actorsSchemaDf = sqlContext.read.format("csv")
  .option("header", "true")
  .schema(schemat)
  .load(filePath)

display(actorsSchemaDf)

//wyswietlenie schematu za pomoca funkcji
actorsDf.printSchema

// COMMAND ----------

// DBTITLE 1,Wczytanie danych do DF z pliku JSON
// U�yj kilku rz�d�w danych z jednego z plik�w csv i stw�rz plik json. Stw�rz schemat danych do tego pliku. Przydatny tool to sprawdzenie formatu danych. https://jsonformatter.curiousconcept.com/

val actorsJson = """
    [{
    "imdb_title_id": "tt0000009",
    "ordering": 1,
    "imdb_name_id": "nm0063086",
    "category": "actress",
    "job": "null",
    "characters": [
      "Miss Geraldine Holbrook (Miss Jerry)"
      ]
  },
  {
    "imdb_title_id": "tt0000009",
    "ordering": 2,
    "imdb_name_id": "nm0183823",
    "category": "actor",
    "job": "null",
    "characters": [
      "Mr. Hamilton"
      ]
  },
  {
    "imdb_title_id": "tt0002844",
    "ordering": 4,
    "imdb_name_id": "nm0137288",
    "category": "actress",
    "job": "null",
    "characters": [
      "Lady Beltham",
      "ma�tresse de Fant�mas"
      ]
  }]
  """

val miniDf =actorsDf.limit(5)

miniDf.write.json("dbfs:/FileStore/tables/miniDf.json")
val miniDf_with_schema = spark.read.schema(schemat).json("dbfs:/FileStore/tables/miniDf.json")
display(miniDf_with_schema)

// COMMAND ----------

// DBTITLE 1,U�ycie Read Modes
//Wykorzystaj posiadane pliki b�d� dodaj nowe i u�yj wszystkich typ�w oraz �badRecordsPath�, zapisz co si� dzieje. Je�li jedna z opcji nie da //�adnych efekt�w, trzeba popsu� dane.

val record1 = "{ppppp}"

val record2 = "{'imdb_title_id': 'tt00003', 'ordering': 1, 'imdb_name_id': 'nm0063486', 'category': 'actress', 'job': 'null', 'characters': ['Miss Geraldine Holbrook (Miss Jerry)']}"

val record3 = "{'imdb_title_id': 'tt0110009', 'ordering': 2, 'imdb_name_id': 'nm003086', 'category': 1, 'job': 'null', 'characters': 'Miss Geraldine Holbrook (Miss Jerry)'}"

Seq(record1, record2, record3).toDF().write.mode("overwrite").text("/FileStore/tables/small_df.json")

val miniDFDropMalFormed = spark.read.format("json")
  .schema(schemat)
  .option("mode", "DROPMALFORMED")
  .load("/FileStore/tables/small_df.json")
val miniDFBadRecord = spark.read.format("json")
  .schema(schemat)
  .option("badRecordsPath", "/FileStore/tables/badrecords")
  .load("/FileStore/tables/small_df.json")
val miniDFPermissive = spark.read.format("json")
  .schema(schemat)
  .option("mode", "PERMISSIVE")
  .load("/FileStore/tables/small_df.json")

val miniDFFailFast = spark.read.format("json")
  .schema(schemat)
  .option("mode", "FAILFAST")
  .load("/FileStore/tables/small_df.json")

display(miniDFDropMalFormed)

// COMMAND ----------

// DBTITLE 1,U�ycie DataFrameWriter
//U�ycie DataFrameWriter.
//Zapisz jeden z wybranych plik�w do format�w (�.parquet�, �.json�). Sprawd�, czy dane s� zapisane poprawnie, u�yj do tego DataFrameReader. //Opisz co widzisz w docelowej �cie�ce i otw�rz u�ywaj�c DataFramereader.

actorsDf.write.format("parquet").mode("overwrite").save("/FileStore/tables/actors_par.parquet")
val actors_par = spark.read.format("parquet").load("/FileStore/tables/actors_par.parquet")
display(actors_par)
//format parquet grupuje rekordy podczas zapisu df, twprzy dzieki temu mniej plikow w folderze,  
//parquet jest nieczytelny dla czlowieka w przeciwienstwie do jsona