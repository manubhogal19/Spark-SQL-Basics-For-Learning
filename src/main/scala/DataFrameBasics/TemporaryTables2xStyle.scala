package SparkSQL.DataFrameBasics

import org.apache.spark.sql.SparkSession

object TemporaryTables2xStyle {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("DF Basic Operations")
      .getOrCreate()

    val df = sparkSession.read.parquet("D:\\IdeaProjects\\SparkSQLProject\\src\\main\\resources\\datasets\\userdata1.parquet")

    df.createTempView("temp_table")
    val sqlDF = sparkSession.sql("select email from temp_table where gender = 'Male'")
    sqlDF.show(10)

    df.createOrReplaceTempView("temp_table")
    val sqlDF2 = sparkSession.sql("select id from temp_table where gender = 'Male'")
    sqlDF2.show(10)


  }

}
