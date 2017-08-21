package cluster

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel, LDA, LDAModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object LDANewsClustering {

  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
  import spark.implicits._

  def getDF() = {


    val df = spark.read.json("data/news.json.gz").filter($"language" === "en").select("canonical_link", "text").distinct().cache
    print(s"total number of records: ${df.count()}")

    df
  }

  def main(args: Array[String]): Unit = {

    val df = getDF()

    val (rescaledDF: DataFrame, vocabulary) = preprocess(df)

    val topics = train(rescaledDF, vocabulary)

    showTopics(topics, vocabulary)
  }


  val udfSize = udf[Int, Seq[String]](_.size)

  def format(seq: Seq[Double]): Seq[String] = {
      seq.map("%.5f".format(_))
  }

  val udfFormat = udf[Seq[String], Vector](vec => format(vec.toArray))
  val udfFormatSeq = udf[Seq[String], Seq[Double]](format(_))


  private def train(rescaledData: DataFrame, vocabulary: Array[String]) = {
    val start = System.currentTimeMillis()

    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(rescaledData)

    val ll = model.logLikelihood(rescaledData)

    val lp = model.logPerplexity(rescaledData)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(maxTermsPerTopic = 8)
    println("The topics described by their top-weighted terms:")
    val udfTerms = udf[Seq[String], Seq[Int]](seq => seq.map(id => vocabulary(id)))
    topics.select($"topic", $"termIndices", udfTerms($"termIndices").as("term"), udfFormatSeq($"termWeights").as("termWeights")).show(false)


    // Shows the result.

    val transformed = model.transform(rescaledData)
    transformed.printSchema()
    transformed.select(col("canonical_link"), udfFormat(col("topicDistribution")).as("topicDistribution")).show(false)
    val r = transformed.head()
    val elapsed = (System.currentTimeMillis() - start) / 1000.0
    println(s"totally spend $elapsed second")

    topics
  }

  def showTopics(topics: DataFrame, vocabArray: Array[String]): Unit = {
//    val topics = model.describeTopics(maxTermsPerTopic = 20)
//    (topic, termIndices.toSeq, termWeights.toSeq)
//    val topics = topicIndices.map { case (terms, termWeights) =>
//      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
//    }
//    println(s"${params.k} topics:")
    topics.collect().foreach { case Row(topic, termIndices: Seq[Int], termWeights: Seq[Double]) =>
      println(s"TOPIC $topic ${termIndices.size} ${termWeights.size}")

      termIndices.zipWithIndex.foreach { case (term, index) =>
        val weight = "%.5f".format(termWeights(index))
        val vocab = vocabArray(term)
        println(s"$weight\t$vocab\t$term")
      }
      println()
    }

  }

  private def preprocess(df: DataFrame) = {
    val regexTokenizer = new RegexTokenizer().
      setInputCol("text").
      setOutputCol("words").
      setPattern("\\W")
    val tokenized = regexTokenizer.transform(df)
    tokenized.show()



    val remover = new StopWordsRemover().
      setInputCol("words").
      setOutputCol("removed").setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
    val removed = remover.transform(tokenized)
    removed.show()

    val stemmer = new Stemmer().setInputCol("removed").setOutputCol("stemmed")
    val stemmed = stemmer.transform(removed)

    stemmed.select($"words",
      udfSize($"words").alias("wordsOriginal"),
      udfSize($"removed").alias("wordsAfterRemoved"),
      udfSize($"stemmed").alias("wordsAfterStemmed")).show()

    val minDF = 0.2
    val minTF = 0.8
    val vocabSize = 2900000
    val countVectorizer = new CountVectorizer().setInputCol("stemmed").
      setOutputCol("features").setMinDF(3).setMinTF(2)//.setVocabSize(vocabSize).setMinDF(minDF).setMinTF(minTF)


    val vectorModel = countVectorizer.fit(stemmed)

    println("vocabulary: " + vectorModel.vocabulary.take(20).mkString(" "))
    println(s"totally ${vectorModel.vocabulary.length} of words")

    val newVectorModel = new CountVectorizerModel(vectorModel.vocabulary.drop(100)).
      setInputCol("stemmed").setOutputCol("features")
    val featurizedData = newVectorModel.transform(stemmed)
    val dfFeaturized: DataFrame = featurizedData.select("canonical_link", "words", "features")


//    val countToken = dfFeaturized.map{
//      row => row.getAs("features")
//    }
    (dfFeaturized.cache(), newVectorModel.vocabulary)
  }

}
