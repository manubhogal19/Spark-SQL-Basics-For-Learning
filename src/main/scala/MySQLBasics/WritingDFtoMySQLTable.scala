package SparkSQL.MySQLBasics

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object WritingDFtoMySQLTable extends App {


  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("Connecting to MySQL from a Spark job")
    .getOrCreate()

  val url = "jdbc:mysql://localhost:3306"
  val tableName = "student_data.count_countrywise"
  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "root")
  properties.put("driver", "com.mysql.cj.jdbc.Driver")

  val df = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .parquet("D:\\IdeaProjects\\SparkSQLProject\\src\\main\\resources\\datasets\\userdata1.parquet")

  df.write.mode(SaveMode.Overwrite).jdbc(url, "parquet_data.userdata", properties)

}
