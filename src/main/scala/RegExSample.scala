import java.security.MessageDigest

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.matching.Regex

object RegExSample {
  def main(args: Array[String]): Unit = {
    val regexes = Map("11 numbers" -> raw"\b\d{11}\b".r)

    def sample(reg: Regex, content: String): Seq[String] = {
      reg.findAllMatchIn(content).map(m => m.matched).toSeq
    }


    val content = "01234567890 0123456789a 12345678900"

    for ((key, reg) <- regexes) {
      val matches = sample(reg, content).mkString(",")
      println(s"$key : $matches")
    }
  }

  def constReplacer(const: String): String => String = {
    anyStr => const
  }

  val digestSHA256: MessageDigest = java.security.MessageDigest.getInstance("SHA-256")
  val hashReplacer: String => String = text => String.format("%064x",
    new java.math.BigInteger(1, digestSHA256.digest(text.getBytes("UTF-8"))))

  def mask(from: Int, to: Int, maskChar: Char): String => String =
    value =>
      value.zipWithIndex.map {
        case (ch: Char, index: Int) =>
          if (index >= from && index < to) maskChar
          else ch
      }.mkString

  case class Anonymizer(name: String, reg: Regex, replacer: String => String)

  val meta = Seq(
    Anonymizer("11 numbers", raw"\b\d{11}\b".r, constReplacer("<phone>")),
    Anonymizer("9 number", raw"\b\d{9}\b".r, hashReplacer),
    Anonymizer("7 number", raw"\b\d{7}\b".r, mask(2, 4, '*'))
  )

//  def anonymize(content: String): String = {
//    meta.foldRight(content)((anon, inter) => anon.reg.
//  }



}
