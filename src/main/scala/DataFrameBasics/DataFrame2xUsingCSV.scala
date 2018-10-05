package SparkSQL.DataFrameBasics

import org.apache.spark.sql.SparkSession

object DataFrame2xUsingCSV {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("DF with CSV, 2.X style")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\IdeaProjects\\SparkSQLProject\\src\\main\\resources\\datasets\\final_data.csv")


    df.printSchema()
    df.show()

  }

}
