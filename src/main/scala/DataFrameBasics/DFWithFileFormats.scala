package SparkSQL.DataFrameBasics

import org.apache.spark.sql.SparkSession

object DFWithFileFormats {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.orc.impl", "native")
      .appName("Working with Multiple File Formats")
      .getOrCreate()
    /*
         val orcDF  = sparkSession.read
         .orc("D:\\IdeaProjects\\SparkSQLProject\\src\\main\\resources\\datasets\\sample.orc")
         orcDF.printSchema()
         orcDF.show()
         println(orcDF.count())

         val parquetDF  = sparkSession.read.parquet("D:\\IdeaProjects\\SparkSQLProject\\src\\main\\resources\\datasets\\userdata1.parquet")
         parquetDF.printSchema()
         parquetDF.show()
         println(parquetDF.count())*/

    val jsonDF = sparkSession.read
      .option("multiLine", "true")
      .json("D:\\IdeaProjects\\SparkSQLProject\\src\\main\\resources\\datasets\\data.json")

    jsonDF.printSchema()
    jsonDF.show()
    println(jsonDF.count())
  }

}
