package cluster


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession

object NewsClustering {

  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()

  def main(args: Array[String]): Unit = {
    val df = spark.read.json("data/news.json.gz").select("text")

    print(s"total number of records: ${df.count()}")

    val regexTokenizer = new RegexTokenizer().
      setInputCol("text").
      setOutputCol("words").
      setPattern("\\W")
    val tokenized = regexTokenizer.transform(df)
    tokenized.show()

    val remover = new StopWordsRemover().
      setInputCol("words").
      setOutputCol("removed")
    val removed = remover.transform(tokenized)
    removed.show()

    val stemmer = new Stemmer().setInputCol("removed").setOutputCol("stemmed")
    val stemmed = stemmer.transform(removed)

    stemmed.show()

    val hashingTF = new HashingTF().setInputCol("stemmed").setOutputCol("hashed").setNumFeatures(73)

    val hashed = hashingTF.transform(stemmed)

    hashed.show()
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("hashed").setOutputCol("features")
    val idfModel = idf.fit(hashed)

    val rescaledData = idfModel.transform(hashed)
    rescaledData.show()
    println(rescaledData.select("hashed", "features").head().mkString("\n"))

    var previousWSSSE = Double.MaxValue
    val rdd = rescaledData.select("features").rdd.map{
      row =>
        row.get(0) match {
          case v: Vector =>
            org.apache.spark.mllib.linalg.Vectors.fromML(v)
        }
    }.cache()

    val useML = false
    val count = rescaledData.count()
    for (k <- 2 to 50) {

      if (useML) {
        val kmeans = new KMeans().setK(k).setSeed(1L).setMaxIter(20)
        val pipeline = new Pipeline().setStages(Array(kmeans))

        val model = pipeline.fit(rescaledData)


        // Evaluate clustering by computing Within Set Sum of Squared Errors.
        val kmmodel = model.stages.head.asInstanceOf[KMeansModel]

        val WSSSE = kmmodel.computeCost(rescaledData) / count

        println(s"WSSSE = $WSSSE when K=$k")
      }
      else {


        val clusters = org.apache.spark.mllib.clustering.KMeans.train(rdd,
          k,
          100)
        val WSSSE = clusters.computeCost(rdd) / count
        println(s"WSSSE = $WSSSE when K=$k")
      }
    }
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

  def explainHashingTF: Unit = {
    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

    val sentenceData = spark.createDataFrame(Seq(
      (0, "Money is important"),
      (1, "Money is not important")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show(false)
  }


}
