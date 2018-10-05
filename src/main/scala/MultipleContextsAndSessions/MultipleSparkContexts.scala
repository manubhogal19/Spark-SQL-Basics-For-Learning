package SparkSQL.MultipleContextsAndSessions

import org.apache.spark.{SparkConf, SparkContext}

object MultipleSparkContexts {
  //This wont work, as multiple spark context are not allowed on same Spark job, this is for learning reference only.

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Multiple Spark Contexts")

    val sc = new SparkContext(sparkConf)
    val sc1 = new SparkContext(sparkConf)

    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))
    val rdd1 = sc1.parallelize(Array(6, 7, 8, 9, 0))

    rdd.collect()
    rdd1.collect()

  }

}
