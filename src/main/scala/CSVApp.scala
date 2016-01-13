/* SimpleApp.scala */

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

object CSVApp {
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)



  val dfTrain = load("train.csv")
  val dfTest = load("test.csv")

  val severity = dfTrain("fault_severity")
  val dfEventType = load("event_type.csv")
  val dfLogFeatures = load("log_feature.csv")
  val dfResourceType = load("resource_type.csv")
  val dfSeverityType = load("severity_type.csv")

  def main(args: Array[String]) {

//    println(severity)
//    val s0 = dfTrain.filter(severity > 1)
//    println(s0.first())
//    println(dfTrain.first())


    val train = join(dfTrain).to
    val eval = join(dfTest)

    train.printSchema()


    // Split data into training (60%) and test (40%).
    val splits = train.randomSplit(Array(0.6, 0.4), seed = 11L)
    MLUtils.
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
//    model.clearThreshold()
//
//    // Compute raw scores on the test set.
//    val scoreAndLabels = test.map { point =>
//      val score = model.predict(point.features)
//      (score, point.label)
//    }
//
//    // Get evaluation metrics.
//    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//    val auROC = metrics.areaUnderROC()
//
//    println("Area under ROC = " + auROC)
//
//    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = SVMModel.load(sc, "myModelPath")

  }

  def join(data: DataFrame): DataFrame = {
    val df = data.join(dfEventType, "id").join(dfLogFeatures, "id").join(dfResourceType, "id").join(dfSeverityType, "id")
    df
  }

  def load(file: String) = {
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("data/" + file)
    df.printSchema()
    df
  }
}