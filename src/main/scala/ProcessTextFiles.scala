import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ProcessTextFiles {

  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
  import spark.implicits._





  private def filterEmptyLines(rdd: RDD[String]): DataFrame = {
    val filtered = rdd.toDF("row").filter(length(trim($"row")) !== 0)

    filtered.show()

    filtered
  }

  private def filterMultipleEmptyLines(rdd: RDD[String]): DataFrame  = {
    val df2 = rdd.toDF("row").
      withColumn("id", monotonically_increasing_id()).
      withColumn("new", lag(col("row"), 1, "").over(Window.orderBy("id")))

    val filtered = df2.filter(
      !(length(trim($"row")) === 0 && length(trim($"new")) === 0)).
      select("row")

    filtered.show()

    filtered
  }

  def main(args: Array[String]): Unit = {

    val rdd = spark.sparkContext.textFile("src/main/resources/fileWithLotsOfEmptyLines.txt")

    val df = filterMultipleEmptyLines(rdd)

    val df2 = filterEmptyLines(rdd)
    //    filtered.coalesce(1).write.text("filtered")
  }
}
