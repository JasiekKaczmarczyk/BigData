package DataReader
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, SparkSessionExtensionsProvider}

object Reader {
  def readData(spark: SparkSession, path: String): DataFrame ={
    val data =  spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    return data.toDF()
  }
}


