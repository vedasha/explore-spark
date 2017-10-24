import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.ml.feature.StopWordsRemover

object StopWords {
  def main(args: Array[String]): Unit = {
    val stop1: Array[String] = StopWordsRemover.loadDefaultStopWords("english")

    val stop2: Array[String] = scala.io.Source.fromFile("src/main/resources/stopWords.txt").mkString.split(",")

    val stop3: Array[String] = scala.io.Source.fromFile("src/main/resources/newsstopwords.txt").getLines().toArray

    val stopWords = (stop1 ++ stop2 ++ stop3).toSet.toArray
    scala.util.Sorting.quickSort(stopWords)

    println(stopWords.size)

    val writer = new BufferedWriter(new FileWriter("stop.txt"))
    stopWords.foreach(word => writer.write(s"$word\n"))
    writer.close()
  }
}