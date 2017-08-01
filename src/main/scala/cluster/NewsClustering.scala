package cluster

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession


object NewsClustering {

  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()

  def main(args: Array[String]): Unit = {
    transform()

  }



  def transform(): Unit = {
    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
    val removed = remover.transform(dataSet)
    removed.show()

    val stemmer = new Stemmer().setInputCol("filtered").setOutputCol("stemmed")
    val stemmed = stemmer.transform(removed)

    stemmed.show()
  }

}
