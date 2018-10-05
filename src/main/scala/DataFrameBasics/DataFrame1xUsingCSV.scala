package SparkSQL.DataFrameBasics

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrame1xUsingCSV {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("DF Using CSV, Spark 1.x style")
      .setMaster("local")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\IdeaProjects\\SparkSQLProject\\src\\main\\resources\\datasets\\final_data.csv")

    df.printSchema()
    df.show()

  }

}
