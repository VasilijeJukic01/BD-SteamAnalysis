import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetReaderTest {

  def main(args: Array[String]): Unit = {
    val parquetFilePath = "output/steam-games.parquet"

    val spark = SparkSession.builder()
      .appName("ParquetReaderTest")
      .master("local[*]")
      .getOrCreate()

    try {
      val parquetDF: DataFrame = spark.read.parquet(parquetFilePath)

      println("Schema:")
      parquetDF.printSchema()
      parquetDF.show(10, truncate = false)
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }
}
