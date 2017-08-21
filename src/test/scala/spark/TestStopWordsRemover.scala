package spark

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.junit.Test

class TestStopWordsRemover {
  val spark = SparkSessionHolder.spark
  import spark.implicits._

  @Test
  def testBasicRemover = {

    val df = spark.createDataFrame(Seq(
      (0, "Money is not important happiness is"),
      (1, "Money is important")
    )).toDF("label", "sentence").withColumn("words", split($"sentence", " "))


    val stopWords = StopWordsRemover.loadDefaultStopWords("english")
    val remover = new StopWordsRemover().
      setInputCol("words").
      setOutputCol("removed").setStopWords(stopWords)

    println("word is location : " + stopWords.indexOf("is"))
    remover.transform(df).show(false)
  }

  @Test
  def testRemoverAfterTokenizer = {

    val df = spark.createDataFrame(Seq(
      (0, "Money is not important happiness is"),
      (1, "Money is important")
    )).toDF("label", "text")

    val regexTokenizer = new RegexTokenizer().
      setInputCol("text").
      setOutputCol("words").
      setPattern("\\W")
    val tokenized = regexTokenizer.transform(df)

    val stopWords = StopWordsRemover.loadDefaultStopWords("english")
    val remover = new StopWordsRemover().
      setInputCol("words").
      setOutputCol("removed").setStopWords(stopWords)

    println("word is location : " + stopWords.indexOf("is"))
    remover.transform(tokenized).show(false)
  }
}
