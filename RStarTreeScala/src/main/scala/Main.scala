import org.apache.spark.{
  SparkConf, SparkContext
}

object Main {

  def main(args: Array[String]): Unit = {
    println("Hello world!")

    val conf = new SparkConf().setMaster("local[*]").setAppName("SkylineRStarTree")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(1,2,3))
    println(rdd.count())
  }
}