import java.security.MessageDigest

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object anonymize {

  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val mask5to7 = mask(5, 7, '*')

    val df = Seq("123456789", "abcdefghi").toDF("value")

    df.registerTempTable("table")
    spark.udf.register("mask", mask5to7)

    spark.sql("select mask(value) from table").show

  }

  def mask(from: Int, to: Int, maskChar: Char): UserDefinedFunction = {
    def mask_(value: String): String = {
      value.zipWithIndex.map {
        case (ch: Char, index: Int) =>
          if (index >= from && index < to) maskChar
          else ch
      }.mkString
    }

    udf[String, String](mask_(_))
  }

  val digestSHA256: MessageDigest = java.security.MessageDigest.getInstance("SHA-256")
  val digestSHA1: MessageDigest = java.security.MessageDigest.getInstance("SHA-1")

//  md.digest("Hoger".getBytes("UTF-8")).map("%02x".format(_)).mkString

  def sha256Hash(text: String) : String =
    String.format("%064x",
      new java.math.BigInteger(1, digestSHA256.digest(text.getBytes("UTF-8"))))

//  def hash(from: Int, to: Int): UserDefinedFunction = {
//    def hash_(value: String): String = {
//      value.take(from) + value.substring(from, to)
//    }
//
//  }
}
