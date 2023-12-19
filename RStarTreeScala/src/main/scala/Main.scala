import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {
    println("Hello world!")

    val conf = new SparkConf().setMaster("local[*]").setAppName("SkylineRStarTree")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(1,2,3))
    println(rdd.count())

    val l = ListBuffer[Int](0,1,2,3,4)
    val splitIndex: Int = (l.size - 1) / 2
    println("Split at = ", splitIndex)
    println(l.take(splitIndex))
    println(l.drop(splitIndex))
  }
}