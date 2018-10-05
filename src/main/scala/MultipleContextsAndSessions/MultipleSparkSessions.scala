package SparkSQL.MultipleContextsAndSessions

import org.apache.spark.sql.SparkSession

object MultipleSparkSessions {
  def main(args: Array[String]): Unit = {

    val sparkSession1 = SparkSession.builder()
      .master("local")
      .appName("Multiple Spark Sessions 2.x")
      .getOrCreate()

    val sparkSession2 = SparkSession.builder()
      .master("local")
      .appName("Multiple Spark Sessions 2.x")
      .getOrCreate()

    val rdd1 = sparkSession1.sparkContext.parallelize(Array(1, 2, 3, 4, 5))
    val rdd2 = sparkSession2.sparkContext.parallelize(Array(6, 7, 8, 9, 0))

    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
  }

}
