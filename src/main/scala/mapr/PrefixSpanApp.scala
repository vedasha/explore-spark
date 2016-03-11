package mapr

/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}

object PrefixSpanApp {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Simple Application")
//      .setMaster("spark://Rockies-MacBook-Pro.local:7077")
        .setMaster("local")
//      .setJars(Seq("/Users/rockieyang/git/explore-spark/target/spark-scala-maven-project-0.0.1-SNAPSHOT.jar"))
    import org.apache.spark.mllib.fpm.PrefixSpan
    val sc = new SparkContext(conf)

//    val sequences = sc.parallelize(Seq(
//      Array(Array(1, 2), Array(3)),
//      Array(Array(1), Array(3, 2), Array(1, 2)),
//      Array(Array(1, 2), Array(5)),
//      Array(Array(6))
//    ), 2).cache()
    val sequences = sc.parallelize(Seq(
      Array(Array(5,1), Array(3)),
      Array(Array(1), Array(3, 2), Array(5,1)),
      Array(Array(5,1), Array(5)),
      Array(Array(6))
    ), 2).cache()


    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
    val model = prefixSpan.run(sequences)
    model.freqSequences.collect().foreach { freqSequence =>
      println(
        freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          ", " + freqSequence.freq)
    }
  }
}