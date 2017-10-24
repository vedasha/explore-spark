import java.io.{File, PrintWriter}

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.extractor
import net.ruippeixotog.scalascraper.model.Document
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.elements

import scala.collection.mutable
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL.Parse._
import net.ruippeixotog.scalascraper.model._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CrawlStateAir {
  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()


  def main(args: Array[String]): Unit = {

    crawl()


//    parse2Dataframe()


  }

  private def crawl() = {
    val browser = JsoupBrowser()
    val rootDoc = browser.get("http://www.stateair.net/web/historical/1/1.html")
    val hists = historicals(rootDoc)

    val allCSVs = hists.map(url => browser.get(url)).flatMap(doc => csvs(doc)).toSet

    val pw = new PrintWriter(new File("result.csv"), "ISO-8859-1")

    var n = 0
    for (csvURL <- allCSVs) {
      val source = scala.io.Source.fromURL(csvURL, "ISO-8859-1")
      val site = csvURL.split("/").last.split("_").head.toLowerCase

      source.getLines().foreach {
        line =>
          println(line)
          val aline = line.trim.toLowerCase()
          if (aline.startsWith(site)) {
            pw.println(aline)
            n += 1
          }
      }
    }


    pw.close
    println(n)
    println(allCSVs.mkString("\n"))

    allCSVs
  }

  def parse2Dataframe(): Unit = {
    case class Record(site: String, parameter: String, date: String, year: Int, month: Int, day: Int, hour: Int, value: Int, unit: String, duration: String, qcname: String, orginal: String)

    def safeInt(s: String): Int = {
      try {
        s.toInt
      } catch {
        case ex => println(s"$s is not a number")
          -1
      }
    }
    import spark.sqlContext.implicits._

    val rdd = spark.sparkContext.textFile("result.csv")
    val original = rdd.map(s => s.split(",")).map(
      s => Record(s(0), // site
        s(1), // parameter
        s(2), // date
        safeInt(s(3)), // year
        safeInt(s(4)), // month
        safeInt(s(5)), // day
        safeInt(s(6)), // hour
        safeInt(s(7)), // value
        s(8),       // unit
        s(9),       // duration
        s(10),       // qcname
        s.mkString(",")
      )
    )//.toDF()

//    original.describe("value").show
//    original.write.csv("dataframe.csv")
  }

  /**
    * Example URL: http://www.stateair.net/web/assets/historical/1/Beijing_2011_HourlyPM25_created20140709.csv
    * @param doc
    * @return
    */
  def csvs(doc: Document): Iterable[String] = {
    val as = doc >> extractor("a", elements)
    val hrefs = as.map(a => a.attr("href"))

    hrefs.filter(href => href.trim.endsWith(".csv"))
  }

  /**
    * sample href : http://www.stateair.net/web/post/1/1.html
    *
    * @param doc
    * @return
    */
  def historicals(doc: Document): Iterable[String] = {
    val as = doc >> extractor("a", elements)
    val hrefs = as.map(a => a.attr("href"))

    hrefs.filter(href => href.contains("post")).map(href => href.replace("post", "historical"))
  }
}
