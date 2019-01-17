
import org.apache.spark.sql.SparkSession
import org.junit.Test

class TestDocGen {
  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()

  @Test
  def test(): Unit ={
    val jsonContent = scala.io.Source.fromFile("src/test/resources/metadoc.json").getLines().mkString("\n")

//    DocGenJSON4S.gen(jsonContent, spark.sparkContext)
  }
}
