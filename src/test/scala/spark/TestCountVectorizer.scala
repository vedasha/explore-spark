package spark

import org.apache.spark.sql.SparkSession
import org.junit.Test
import org.apache.spark.ml.feature._

class TestCountVectorizer {
  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()

  import spark.implicits._

  val seq10 = Seq("A", "B", "C", "D", "E", "F", "G", "H", "I", "J")
  val df = (1 to 10).map {
    i => (i, seq10.take(i))
  }.toDF("id", "words")

  println("Source DataFrame")
  df.show(false)
  //    +---+------------------------------+
  //    |id |words                         |
  //    +---+------------------------------+
  //    |1  |[A]                           |
  //    |2  |[A, B]                        |
  //    |3  |[A, B, C]                     |
  //    |4  |[A, B, C, D]                  |
  //    |5  |[A, B, C, D, E]               |
  //    |6  |[A, B, C, D, E, F]            |
  //    |7  |[A, B, C, D, E, F, G]         |
  //    |8  |[A, B, C, D, E, F, G, H]      |
  //    |9  |[A, B, C, D, E, F, G, H, I]   |
  //    |10 |[A, B, C, D, E, F, G, H, I, J]|
  //    +---+------------------------------+

  @Test
  def testIncludeAllVocab = {
    //
    // Include all words in the vocabulary
    //
    val model = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").fit(df)
    val usage =
      s"""Include all words in the vocabulary
         |  Vocabulary: ${model.vocabulary.mkString(" ")}""".stripMargin
    println("\n" + usage)
    model.transform(df).show(false)
  }

  @Test
  def testMinTF30Percent: Unit = {
    //
    // Minimum term frequency: 30 %
    //
    val model = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").
      setMinTF(0.3).fit(df)
    val usage =
      s"""Minimum term frequency: 30 %
         |   The term in the document shall appear more than 30%
         |   The 1st document only has A, which A has 100%
         |   The 2nd document has A and B, each has 50% weight
         |   The 3rd document has A B C, each has 33% weight
         |   The 4th document has A B C D, each has 25% weight, which is less than 30%
         |   Vocabulary: ${
        model.
          vocabulary.mkString(" ")
      }""".stripMargin
    println("\n" + usage)
    model.transform(df).show(false)
  }

  @Test
  def testMinDF30Percent = {
    //
    // Minimum document frequency: 30 %
    //
    val model = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").
      setMinDF(0.3).fit(df)
    val usage =
      s"""Minimum document frequency: 30 %
         |   Terms shall appear in more than 30% of document
         |   J only appear in 1 document, which is 10%
         |   I only appear in 1 document, which is 20%
         |   Vocabulary: ${model.vocabulary.mkString(" ")}""".stripMargin
    println("\n" + usage)
    model.transform(df).show(false)
  }

  @Test
  def testMinTF3 = {
    //
    // Minimum term frequency: 3
    //
    val model = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").
      setMinTF(3).fit(df)
    val usage =
      s"""Minimum term frequency: 3
         |   Terms shall repeat 3 times in each document
         |   In the example, each only appear once, so all terms are excluded
         |   Vocabulary:  ${model.vocabulary.mkString(" ")}""".stripMargin
    println("\n" + usage)
    model.transform(df).show(false)
  }

  @Test
  def testMinDF3 = {
    //
    // Minimum document frequency: 3
    //
    val model = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").
      setMinDF(3).fit(df)
    val usage =
      s"""Minimum document frequency: 3
         |   Terms shall appears in more than 3 documents
         |   J only appear in 1 document, so is excluded
         |   I only appear in 2 documents, so is excluded
         |   Vocabulary: ${model.vocabulary.mkString(" ")}""".stripMargin
    println("\n" + usage)
    model.transform(df).show(false)
  }

  @Test
  def testVocabSize3 = {
    //
    // Limit VocabSize: 3
    //
    val model = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").
      setVocabSize(3).fit(df)
    val usage =
      s"""Limit VocabSize: 3
         |   Vocabulary: ${model.vocabulary.mkString(" ")}""".stripMargin
    println("\n" + usage)
    model.transform(df).show(false)
  }

  def testVocabSize3MinTF3 = {
    //
    // Limit VocabSize: 3, MinTF3
    //
    val model = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").
      setVocabSize(3).setMinTF(3).fit(df)
    val usage =
      s"""Limit VocabSize: 3, MinTF3
         |   All terms in the documents only appear once, so they are all excluded
         |   Vocabulary: ${model.vocabulary.mkString(" ")}""".stripMargin
    println("\n" + usage)
    model.transform(df).show(false)
  }

  @Test
  def testVocabSize3MinDF3 = {
    //
    // Limit VocabSize: 3, MinDF3
    //
    val model = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").
      setVocabSize(3).setMinDF(3).fit(df)
    val usage =
      s"""Limit VocabSize: 3, MinDF3
         |   A - H are appeared in more than 3 documents,
         |   While we limited to 3 vocabulary size, so only A B C are coded
         |   Vocabulary: ${model.vocabulary.mkString(" ")}""".stripMargin
    println("\n" + usage)
    model.transform(df).show(false)
  }

  @Test
  def testSimExcludeMostCommonVocab: Unit = {
    //
    // this assume that vocabulary are ordered by commonality
    val model = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").fit(df)
    val vocabulary = model.vocabulary

    val model2 = new CountVectorizerModel(model.uid, vocabulary.drop(3)).setInputCol("words").setOutputCol("rawFeatures")
    model2.transform(df).show(false)
  }
}
