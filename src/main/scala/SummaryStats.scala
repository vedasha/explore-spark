import java.security.InvalidParameterException

class SummaryStats(values: List[Int]) {
  if (values.isEmpty) {
    throw new InvalidParameterException("empty list does not have statistics")
  }
  val sz = values.size

  lazy val mean: Double = values.map(_.toDouble).sum / sz

  lazy val range: (Int, Int) =
    values.foldLeft((Integer.MAX_VALUE, Integer.MIN_VALUE)){(res, cur)
      => (Math.min(res._1, cur), Math.max(res._2, cur))}

  lazy val rangeWithLoop: (Int, Int) = {
    var min = Integer.MAX_VALUE
    var max = Integer.MIN_VALUE

    for (v <- values) {
      min = Math.min(v, min)
      max = Math.max(v, max)
    }
    (min, max)
  }

  lazy val deviation: Double = {
    if (sz == 1)
      0.0
    else {
      val powerSum = values.map(v => Math.pow(v.toDouble - mean, 2)).sum
      Math.sqrt(powerSum / sz)
    }
  }

  def getMean: Double = mean
  def getRange: (Int, Int) = range
  def getStandardDeviation: Double = deviation

}

object SummaryStats
{
  def main(args: Array[String]): Unit = {
    val nums = 1 to 10000000

    val start = System.currentTimeMillis()
    val s = new SummaryStats(nums.toList)
    for (i <- 1 to 50) {
      val istart = System.currentTimeMillis()
      s.deviation
      println(s"elapsed ${System.currentTimeMillis() - istart}")
    }

    println(s"elapsed ${System.currentTimeMillis() - start}")
  }
}