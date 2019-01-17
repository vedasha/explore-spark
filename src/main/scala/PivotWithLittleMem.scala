import java.io.{File, FileReader, PrintWriter}

import scala.collection.mutable
import scala.io.Source

object PivotWithLittleMem {

  def main(args: Array[String]): Unit = {
    val s = "1231231"

    val minMaskLen = 6
    val notMaskPrefix = 1
    val notMaskSuffix = 4

    def mask(creditCardNumber: String): String = {
      val len = creditCardNumber.length
      if (len < minMaskLen) {
        creditCardNumber
      }
      else {
        creditCardNumber.zipWithIndex.map {
          case (ch, i) => if (i < notMaskPrefix || i >= len - notMaskSuffix || !Character.isDigit(ch)) ch else '#'
        }.mkString
      }
    }

    println(mask("12131s12312s"))
    println(mask("54321"))
//    println("pivot will little memory")
//
//    process(srcFilename = "pivot-source.csv",
//      dstFilename = "pivot-dest.csv",
//      header = "key1,key2,pivot,value",
//      numKeys = 2,
//      pivotPosition = 2,
//      valuePosition = 3,
//      pivots = Array("A", "B", "C", "D"),
//      delimiter = ","
//    )
  }

  def process(srcFilename: String,
              dstFilename: String,
              header: String,
              numKeys: Int,
              pivotPosition: Int,
              valuePosition: Int,
              pivots: Array[String],
              delimiter: String = ""): Unit = {

    val emptyRecord = pivots.map(_ -> "")

    val emptyKeys: Array[String] = Array()
    var currentKeys: Array[String] = emptyKeys

    var record = mutable.Map[String, String](emptyRecord:_*)

    val src = Source.fromFile(srcFilename)
    val dst = new PrintWriter(new File(dstFilename))

    var i = 0
    for (line <- src.getLines()) {
      i += 1
      if (i % 1000000 == 0) {
        print("M")
      }
      else if (i % 100000 == 0) {
        print(".")
      }

      if (line != header) {
        val items = line.split(delimiter)

        val pivot = items(pivotPosition)
        val value = items(valuePosition)

        val newKeys = items.take(numKeys)

        if (currentKeys.sameElements(newKeys)) { // it's still old record
          record(pivot) = value
        } else if (currentKeys.sameElements(emptyKeys)) { // the first new record
          currentKeys = newKeys
          record(pivot) = value
        } else { // it's new record
          // output the previous record
          output(dst, currentKeys, record)

          record = mutable.Map[String, String](emptyRecord:_*)
          currentKeys = newKeys
          record(pivot) = value
        }
      }
    }

    output(dst, currentKeys, record)

    src.close()
    dst.close()
  }

  def output(writer: PrintWriter, keys: Array[String], values: mutable.Map[String, String]): Unit = {
    val items = keys ++ values.toArray.map(_._2)
    writer.println(items.mkString(","))
  }

}
