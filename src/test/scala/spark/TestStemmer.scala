package spark

import cluster.Stemmer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.split
import org.junit.Test

class TestStemmer {
  val spark = SparkSessionHolder.spark
  import spark.implicits._

  @Test
  def testBasicStemmer = {

    val df = spark.createDataFrame(Seq(
      (0, "Money is not important happiness is"),
      (1, "Money is important")
    )).toDF("label", "sentence").withColumn("words", split($"sentence", " "))


    val stemmer = new Stemmer().setInputCol("words").setOutputCol("stemmed")
    val stemmed = stemmer.transform(df)

    stemmed.show(false)
  }
}
