package SparkSQL.MySQLBasics

import java.util.Properties

import org.apache.spark.sql.SparkSession

object QueryPushDownToMySQLDB extends App {


  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("Connecting to MySQL from a Spark job")
    .getOrCreate()

  val url = "jdbc:mysql://localhost:3306"
  val tableName = "student_data.count_countrywise"
  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "root")
  properties.put("driver", "com.mysql.jdbc.Driver")

  val query = "select No_Of_Indian_Students, count(*) as student_count from student_data.count_countrywise group by No_Of_Indian_Students"
  val queryResult = sparkSession.read.jdbc(url, s"($query) as temp_table", properties)

  queryResult.show()


}
