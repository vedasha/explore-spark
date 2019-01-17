import org.apache.spark
import org.apache.spark.sql.SparkSession

object DataProfiler {

  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    println("Hello")

    val filename="src/main/resources/tickets.csv"
    val rdd = spark.read.textFile(filename)

    val transformed = rdd.map(line => {
      println(line)
      line.split(",").mkString("\t\t")
    }
    )

    println(transformed.head())
  }

  val delimiter = ","
  val header = true

  def load(filename: String) = {
    val original = spark.read.textFile(filename)

  }

}
