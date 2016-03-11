package mapr

/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}

object SFPDApp {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Simple Application")
//      .setMaster("spark://Rockies-MacBook-Pro.local:7077")
        .setMaster("local")
//      .setJars(Seq("/Users/rockieyang/git/explore-spark/target/spark-scala-maven-project-0.0.1-SNAPSHOT.jar"))


    /*_________________________________________________________________________*/
    /************Lab 4.1.2 - Load Data into Apache Spark **********/
    //Map input variables
    val IncidntNum = 0
    val Category = 1
    val Descript = 2
    val DayOfWeek = 3
    val Date = 4
    val Time = 5
    val PdDistrict = 6
    val Resolution = 7
    val Address = 8
    val X = 9
    val Y = 10
    val PdId = 11

    implicit object Order2 extends math.Ordering[(String, Int)] {
      def compare(x: (String, Int), y: (String, Int)): Int = {
        y._2.compare(x._2)
      }
    }
    //Load SFPD data into RDD
//    val sfpdRDD = sc.textFile("/path/to/file/sfpd.csv").map(line=>line.split(","))
    val sc = new SparkContext(conf)
    val sfpdFile = "mapr/sfpd.csv"
    val inputRDD = sc.textFile(sfpdFile)
    val mapRDD = inputRDD.map(line => line.split(","))
    val category = 1
    val map2RDD = mapRDD.map(row => (row(category), 1))
    val reduceRDD = map2RDD.reduceByKey((a, b) => a + b)
//    val top5 = reduceRDD.takeOrdered(5)(Order2)

    val sortedRDD = reduceRDD.sortBy(_._2, ascending = false)
    val top5ViaSorted = sortedRDD.take(5)

//    println(sortedRDD.toDebugString)
    println(sortedRDD.take(5).mkString("\n"))
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}