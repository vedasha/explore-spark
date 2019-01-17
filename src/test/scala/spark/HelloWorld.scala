package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()

    val items = Seq("123/643/7563/2134/ALPHA", "2343/6356/BETA/2342/12", "23423/656/343")

    val input = spark.sparkContext.parallelize(items)

    // spark2 change api for flatMap
    // it requires an iterator
    // spark 2.2
    //    public interface FlatMapFunction<T, R> extends Serializable {
    //        Iterator<R> call(T var1) throws Exception;
    //    }

    // spark 1.6
    //    public interface FlatMapFunction<T, R> extends Serializable {
    //        Iterable<R> call(T t) throws Exception;
    //    }

    val sumOfNumbers = input.flatMap { _.split("/") }
      .filter { it => it.matches("[0-9]+") }
      .map { _.toInt }
      .sum()

    println(sumOfNumbers)

  }
}
