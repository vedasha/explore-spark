package cluster

import java.io.FileWriter

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object LDANewsClusteringCoreNLP {

  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
  import spark.implicits._

  def getDF() = {
    val df = spark.read.json("data/cleaned.news.json.gz").select("canonical_link", "text").distinct().cache
    print(s"total number of records: ${df.count()}")

    df
  }

  def main(args: Array[String]): Unit = {

    val df = getDF()

    val (rescaledDF: DataFrame, vocabulary) = preprocess(df)

    val writer =  new FileWriter("vocabulary.txt")
    vocabulary.foreach(vocab => writer.write(s"$vocab\n"))
    rescaledDF.cache()
    val maxIter = 200
    for (i <- 7 to 10) {
      val (topics, lp) = train(rescaledDF, i, maxIter, vocabulary)

      topics.repartition(1).write.json(s"topics$i.$lp.json")
      showTopics(topics, vocabulary)
    }
  }

  val nDropMostCommon = 1000
  val maxTermsPerTopic = 100
  val dropRightPercentage = 0.5

  val udfSeqSize = udf[Int, Seq[String]](_.size)

  def format5Digit(seq: Seq[Double]): Seq[String] = {
    seq.map("%.5f".format(_))
  }

  val udfFormatVector5Digit = udf[Seq[String], Vector](vec => format5Digit(vec.toArray))
  val udfFormatSeq5Digit = udf[Seq[String], Seq[Double]](format5Digit(_))


  private def train(rescaledData: DataFrame, k: Int, iter: Int, vocabulary: Array[String]): (DataFrame, Double) = {
    val start = System.currentTimeMillis()

    val lda = new LDA().setK(k).setMaxIter(iter)

    val model = lda.fit(rescaledData)

    val ll = model.logLikelihood(rescaledData)

    val lp = model.logPerplexity(rescaledData)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(maxTermsPerTopic)

    println("The topics described by their top-weighted terms:")
    val udfTermIndex2Term = udf[Seq[String], Seq[Int]](seq => seq.map(id => vocabulary(id)))
    topics.select(
      $"topic",
      $"termIndices",
      udfTermIndex2Term($"termIndices").as("term"),
      udfFormatSeq5Digit($"termWeights").as("termWeights")
    ).show(false)


    // Shows the result.

    val transformed = model.transform(rescaledData)

    transformed.printSchema()
    transformed.select(
      col("canonical_link"),
      udfFormatVector5Digit(col("topicDistribution")).as("topicDistribution")
    ).show(false)

    val elapsed = (System.currentTimeMillis() - start) / 1000.0
    println(s"totally spend $elapsed second")

    (topics, lp)
  }

  def showTopics(topics: DataFrame, vocabArray: Array[String]): Unit = {


    topics.collect().foreach {
      case Row(topic, termIndices: Seq[Int], termWeights: Seq[Double]) =>
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
    //
    // tokenizer
    //
    val coreNLP = new CoreNLP().setInputCol("text").setOutputCol("words")

    val tokenized = coreNLP.transform(df).filter(size($"words") > 50)

    //
    // words removal
    //
    // remove english stop word and numbers
    val numbers: Array[String] = (0 to 3000).map(_.toString).toArray
    val a2z = ('a' to 'z').map(_.toString).toArray

    val stopWords: Array[String] = StopWordsRemover.loadDefaultStopWords("english") ++ numbers ++ a2z
    val remover = new StopWordsRemover().
      setInputCol("words").
      setOutputCol("removed").setStopWords(stopWords)
    val removed = remover.transform(tokenized)
    removed.show()




    //
    // vectorizer
    //
    // val minDF = 0.2 // collected term appear more than 20% of the documents of the corpus
    // val minDF = 2   // collected term appear more than 2 documents of the corpus
    // val minTF = 0.2 // term having more than 20% appearance in the documents
    // val minTF = 2 // term having appear more than 2 in the documents
    //
    //    val vocabSize = 2900000
    val countVectorizer = new CountVectorizer().setInputCol("removed").
      setOutputCol("features").setMinDF(3).setMinTF(2)

    //
    // fit to get the vocabulary
    //
    // the vocabulary are ordered by commonality
    //
    removed.cache()
    val vectorModel = countVectorizer.fit(removed)

    println("most common words: " + vectorModel.vocabulary.take(20).mkString(" "))
    println(s"totally ${vectorModel.vocabulary.length} of words in the vocabulary")

    //
    // having a new vector model by dropping the some most common words
    //

    val dropRight = (vectorModel.vocabulary.size * dropRightPercentage).toInt
    val vocabulary = vectorModel.vocabulary.drop(nDropMostCommon).dropRight(dropRight)
    val newVectorModel = new CountVectorizerModel(vocabulary).
      setInputCol("removed").setOutputCol("features")

    //
    // transform from words to vectors
    //
    val featurizedData = newVectorModel.transform(removed)
    removed.unpersist()
    val dfFeaturized: DataFrame = featurizedData.select("canonical_link", "words", "features")

    (dfFeaturized, vocabulary)
  }

}
