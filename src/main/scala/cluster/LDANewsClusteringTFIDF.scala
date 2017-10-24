package cluster

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

    val writer =  new FileWriter("vocabulary.txt")
    vocabulary.foreach(vocab => writer.write(s"$vocab\n"))

    val maxIter = 200
    for (i <- 63 to 300) {
      val (topics, lp, ll) = train(rescaledDF, i, maxIter, vocabulary)

      topics.repartition(1).write.json(s"topics$i.$lp.$ll.json")
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


  private def train(rescaledData: DataFrame, k: Int, iter: Int, vocabulary: Array[String]): (DataFrame, Double, Double) = {
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

    (topics, lp, ll)
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
    val regexTokenizer = new RegexTokenizer().
      setInputCol("text").
      setOutputCol("words").
      setPattern("\\W")
    val tokenized = regexTokenizer.transform(df)
    tokenized.show()

    val SMALLEST_ARTICLES = 100
    val dfRemovedSmallArticles = tokenized.filter(size($"words") > SMALLEST_ARTICLES)


    val udfDistinct = udf[Seq[String], Seq[String]](ar => ar.distinct)
//    val dfDistinct = tokenized.select(udfDistinct($"words").as("words"))

//    tokenized.map(row => row)
    //
    // words removal
    //
    // remove english stop word and numbers
    val numbers: Array[String] = (0 to 3000).map(_.toString).toArray
    val a2z = ('a' to 'z').map(_.toString).toArray



    val stopWords: Array[String] = scala.io.Source.fromFile("src/main/resources/stop.txt").getLines().toArray

    val remover = new StopWordsRemover().
      setInputCol("words").
      setOutputCol("removed").setStopWords(stopWords ++ numbers ++ a2z)
    val removed = remover.transform(tokenized)
    removed.show()

    //
    // Stemming
    //
    // TODO how to handle this, after stemming, there are some words like n, th,
    // One way will be having another removal
    val stemmer = new Stemmer().setInputCol("removed").setOutputCol("stemmed")
    val stemmed = stemmer.transform(removed)

    //
    // vectorizer
    //
    // val minDF = 0.2 // collected term appear more than 20% of the documents of the corpus
    // val minDF = 2   // collected term appear more than 2 documents of the corpus
    // val minTF = 0.2 // term having more than 20% appearance in the documents
    // val minTF = 2 // term having appear more than 2 in the documents
    //

    // remove term appear less than 10 articles
    // remove term appear more than 10% of the articles
    //
//    val vocabSize = 2900000
    val countVectorizer = new CountVectorizer().setInputCol("stemmed").
      setOutputCol("features").setMinDF(10).setMinTF(2)

    //
    // fit to get the vocabulary
    //
    // the vocabulary are ordered by commonality
    //
    val vectorModel = countVectorizer.fit(stemmed)
    val featurizedData = vectorModel.transform(stemmed)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)

    val originVocabuary = vectorModel.vocabulary

    println("most common words: " + originVocabuary.take(20).mkString(" "))


    val nums = (0 to 9).map(_.toChar).toSet
    val filteredVocabs = originVocabuary.
      map(vocab => vocab.filter(ch => !nums.contains(ch))).
      filter(vocab => vocab.size > 2)

    println(s"totally ${originVocabuary.length} of words in the vocabulary")
    println(s"totally ${filteredVocabs.length} of words in the vocabulary after filtering")

    val writer = new BufferedWriter(new FileWriter("src/main/resources/vocabulary.txt"))
    filteredVocabs.foreach(word => writer.write(s"$word\n"))
    writer.close()

    //
    // having a new vector model by dropping the some most common words
    //

    val dropRight = (filteredVocabs.size * dropRightPercentage).toInt
    val vocabulary = filteredVocabs.drop(nDropMostCommon).dropRight(dropRight)
    val newVectorModel = new CountVectorizerModel(vocabulary).
      setInputCol("stemmed").setOutputCol("features")

    //
    // transform from words to vectors
    //
    val featurizedData = newVectorModel.transform(stemmed)
    val dfFeaturized: DataFrame = featurizedData.select("canonical_link", "words", "features")
    dfFeaturized.repartition(8).write.parquet("vectorized.parquet")
    val dfParquet = spark.read.parquet("vectorized.parquet").cache()
    (dfParquet, vocabulary)
  }

}
