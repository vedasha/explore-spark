package spark

import org.apache.spark.sql.SparkSession
import org.junit.Test

class TFIDF {
  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()

  @Test
  def explainCountVectorizer: Unit = {
    import org.apache.spark.ml.feature._

    val sentenceData = spark.createDataFrame(Seq(
      (0, "Money is not important happiness is"),
      (1, "Money is important")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val countVectorizer = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures")
    val vectorModel = countVectorizer.fit(wordsData)
    println(vectorModel.vocabulary.mkString(" "))

    val featurizedData = vectorModel.transform(wordsData)
    featurizedData.select("words", "rawFeatures").show(false)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("words", "rawFeatures", "features").show(false)
  }

  @Test
  def explainTFIDF: Unit = {
    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "money is not important happiness is"),
      (0.0, "money is important")
    )).toDF("label", "sentence")

    //
    // split sentence to words
    //
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.select("words").show(false)
    //    +-----------------------------------------+
    //    |words                                    |
    //    +-----------------------------------------+
    //    |[money, is, not, important, happiness, is]|
    //    |[money, is, important]                   |
    //    +-----------------------------------------+

    //
    // Hash term frequency
    //
    // word => hash => count how many time the hash value appears
    //
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(23)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.select("words", "rawFeatures").show(false)
    //    +------------------------------------------+-----------------------------------------+
    //    |words                                     |rawFeatures                              |
    //    +------------------------------------------+-----------------------------------------+
    //    |[money, is, not, important, happiness, is]|(23,[2,3,13,14,19],[2.0,1.0,1.0,1.0,1.0])|
    //    |[money, is, important]                    |(23,[2,13,14],[1.0,1.0,1.0])             |
    //    +------------------------------------------+-----------------------------------------+
    //
    // We set hash bucket size 23
    // the first sentence has hash 2,3,13,14,19; hash value 2 is the word "is" in this case and appear twice
    //                                           other words appear once
    // the second sentence has hash 2,13,14; all appear just once
    //

    //
    // Multiple words might fall into the the hash value
    // if we set the number of features to 20
    // the word "happiness" fall in the the same hash value as "is"
    //
    val hashingTF2 = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData2 = hashingTF2.transform(wordsData)
    featurizedData2.select("words", "rawFeatures").show(false)
    //    +------------------------------------------+----------------------------------+
    //    |words                                     |rawFeatures                       |
    //    +------------------------------------------+----------------------------------+
    //    |[money, is, not, important, happiness, is]|(20,[1,2,17,18],[3.0,1.0,1.0,1.0])|
    //    |[money, is, important]                    |(20,[1,2,17],[1.0,1.0,1.0])       |
    //    +------------------------------------------+----------------------------------+

    //
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    //

    //
    // Inverse Document Frequency
    // idf(t, D) = log((N + 1) / (n + 1))
    //
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("words", "rawFeatures", "features").show(false)
    //+------------------------------------------+-------------------------------+-----------------------------------------------------------------+
    //|words                                     |rawFeatures                    |features                                                         |
    //+------------------------------------------+-------------------------------+-----------------------------------------------------------------+
    //|[money, is, not, important, happiness, is]|(23,[2,3,13,14,19],[2,1,1,1,1])|(23,[2,3,13,14,19],[0,0.4054651081081644,0,0,0.4054651081081644])|
    //|[money, is, important]                    |(23,[2,13,14],[1,1,1])         |(23,[2,13,14],[0,0,0])                                           |
    //+------------------------------------------+-------------------------------+-----------------------------------------------------------------+
    //
    // there are totally 2 documents in the corpus, so N = 2
    // term "not" and "happiness" appear only in the first document, so n = 1
    // term "money" "is" "important appear in both documents, so n = 2
    //
    // idf(t, D)not,happiness = log ( (N + 1) / (n + 1)) = log ( (2 + 1) / (1 + 1) ) = log(1.5) = 0.405
    //
    // idf(t,D)money,is,important = log ( (N + 1) / (n + 1)) = log ( (2 + 1) / (2 + 1) ) = log(1) = 0

  }
}
