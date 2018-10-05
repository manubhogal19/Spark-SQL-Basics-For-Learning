package SparkSQL.DataSetBasics

import org.apache.spark.sql.SparkSession

//case class StudentCountCountrywise(Country: String, No_Of_Indian_Students: Integer)
// we only need to define a particular case class once in a package, this is defined in DataSetBasics.scala

object BasicDataSetOperations extends App {

  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("DF Basic Operations")
    .getOrCreate()

  import sparkSession.implicits._

  val studentDS = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("D:\\IdeaProjects\\SparkSQLProject\\src\\main\\resources\\datasets\\final_data.csv")
    .as[StudentCountCountrywise]
  val filterDemo = studentDS.filter(studentDSOBJ => studentDSOBJ.No_of_Indian_Students > 100)
  val whereDemo = studentDS.where("No_Of_Indian_Students>=100")
  filterDemo.show() //filter in DS
  val selectDemo = studentDS.select("Country").as[CountryName]
  whereDemo.show() //where clause usage on DS

  case class StudentCountCountrywise(Country: String, No_of_Indian_Students: Long)

  /*
  select always returns DF as DS is based on case class and there is no case class for selected columns i.e. Country.
  So we need to create another case class with reduced schema of attributes we need
   */
  case class CountryName(Country: String)

  selectDemo.show()

}
