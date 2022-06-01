package DataWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataWriter {
  def writeData(dataframe: DataFrame, path: String): Unit = {
    dataframe.write.format("csv").save(path)
  }
}

