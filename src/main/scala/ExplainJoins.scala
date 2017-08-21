package cluster

import org.apache.spark.sql.SparkSession

object ExplainJoins {
  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]) {
    val dfL = Seq(1, 2, 3, 4).map(i => (i, s"L$i")).toDF("id", "left")
    val dfR = Seq(4, 5, 6, 7).map(i => (i, s"R$i")).toDF("id", "right")

    val joins = Seq("inner", "outer", "full", "full_outer", "left", "left_outer", "right",
      "right_outer", "left_semi", "left_anti")

//    case "inner" => Inner
//    case "outer" | "full" | "fullouter" => FullOuter
//    case "leftouter" | "left" => LeftOuter
//    case "rightouter" | "right" => RightOuter
//    case "leftsemi" => LeftSemi
//    case "leftanti" => LeftAnti
//    case "cross" => Cross
//    case _ =>
//      val supported = Seq(
//        "inner",
//        "outer", "full", "fullouter",
//        "leftouter", "left",
//        "rightouter", "right",
//        "leftsemi",
//        "leftanti",
//        "cross")

    joins.map { join =>
      {
        println(join);
        dfL.join(dfR, Seq("id"), join).show
      }
    }

    println("cross product, or cross join")
    dfL.crossJoin(dfR).show()

    print("full out join and exclude the intersect one. Through it does not make lots of sense")
    dfL.registerTempTable("TL")
    dfR.registerTempTable("TR")
    spark.sql("select * from TL l full outer join TR r on l.id=r.id where l.id is null or r.id is null").show()
  }

}
