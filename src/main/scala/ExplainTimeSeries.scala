import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import com.cloudera.sparkts.{BusinessDayFrequency, DateTimeIndex, TimeSeriesRDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object ExplainTimeSeries {
  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
//    val sqlContext = spark.sqlContext
//    val path = "src/main/resources/tickets.tsv"
//
//    val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
//      val tokens = line.split('\t')
//      val dt = ZonedDateTime.of(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, 0, 0, 0, 0,
//        ZoneId.systemDefault())
//      val symbol = tokens(3)
//      val price = tokens(5).toDouble
//      Row(Timestamp.from(dt.toInstant), symbol, price)
//    }
//    val fields = Seq(
//      StructField("timestamp", TimestampType, true),
//      StructField("symbol", StringType, true),
//      StructField("price", DoubleType, true)
//    )
//    val schema = StructType(fields)
//    val df = sqlContext.createDataFrame(rowRdd, schema)
//    df.printSchema()
//    df.coalesce(1).write.option("header", "true").csv("src/main/resources/tickets")

    val df=spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/tickets.csv")
    df.printSchema()
    df.sort("timestamp")show()

    // Create an daily DateTimeIndex over August and September 2015
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2015-08-03T00:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2015-09-22T00:00:00"), zone),
      new BusinessDayFrequency(1))

    // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
    val tickerTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, df,
      "timestamp", "symbol", "price")

    // Cache it in memory
    tickerTsrdd.cache()

    // Count the number of series (number of symbols)
    println(tickerTsrdd.count())


    // Impute missing values using linear interpolation
    val filled = tickerTsrdd.fill("linear")
    println(filled.take(5).mkString("\n"))
    val df2 = filled.toDF("timestamp", "symbol", "price")
    df2.sort("timestamp").show()

    // Compute return rates
    val returnRates = filled.returnRates()

    // Compute Durbin-Watson stats for each series
    val dwStats = returnRates.mapValues(TimeSeriesStatisticalTests.dwtest)

    println(dwStats.map(_.swap).min)
    println(dwStats.map(_.swap).max)

  }
}
