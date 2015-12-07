/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SQLContext

object CSVApp {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("data/train.csv")

    df.printSchema
    val severity = df("fault_severity")

    println(severity)
    val s0 = df.filter(severity > 1)
    println(s0.first())
    println(df.first())
  }
}