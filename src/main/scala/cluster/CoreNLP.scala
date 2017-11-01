package cluster

import edu.stanford.nlp.simple.{Document, Sentence}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.JavaConverters._

class CoreNLP(override val uid: String) extends Transformer {
  final val inputCol = new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
  final val stemmer = new StanfordNLPStemmer()

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def this() = this(Identifiable.randomUID("configurablewordcount"))

  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField($(outputCol), IntegerType, false))
  }



  def transform(df: Dataset[_]): DataFrame = {
    val keepPoses = Set("N", "V")

    def collect_(wordPoses: Seq[(String, String)]): Seq[String] = {
      wordPoses.collect { case (lemma: String, pos: String) if keepPoses.contains(pos.take(1)) && lemma.size > 2=>
        lemma
      }
    }


    def nlp_(document: String): Seq[String] = {
      val sentences = new Document(document).sentences().asScala

      sentences.flatMap(sentence =>
        {
          val wordPoses = sentence.lemmas().asScala zip sentence.posTags.asScala
          collect_(wordPoses)
        }
      )
    }

    val nlp = udf{doc: String => nlp_(doc.replaceAll("[^\\x20-\\x7e]", ""))}

    df.select(col("*"), nlp(df.col($(inputCol))).as($(outputCol)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}