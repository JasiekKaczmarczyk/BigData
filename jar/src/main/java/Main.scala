import org.apache.spark.sql.SparkSession
import DataReader.Reader.readData
import DataWriter.DataWriter.writeData
import Transformations.Transformations.{specificJob, heightLessThan}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("maven")
      .getOrCreate()

    val actorsDf = readData(spark, args(0));
    val namesDf = readData(spark, args(1));

    val actorsFiltered = actorsDf.filter(row => specificJob(row, "producer"))
    val namesFiltered = namesDf.filter(row => heightLessThan(row, 165))

    actorsFiltered.show()
    namesFiltered.show()

    writeData(actorsFiltered, args(2));
    writeData(namesFiltered, args(3));
  }
}

