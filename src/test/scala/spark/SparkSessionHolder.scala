package spark

import org.apache.spark.sql.SparkSession

object SparkSessionHolder {
  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
}
