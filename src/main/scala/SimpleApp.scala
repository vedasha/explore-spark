/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "data/README.md" // Should be some file on your system
//    val master = "local"
    val master = "spark://Rockies-MacBook-Pro.local:7077"
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster(master)
      .setJars(Seq("/Users/rockieyang/git/explore-spark/target/spark-scala-maven-project-0.0.1-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    println(logData.first())
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}