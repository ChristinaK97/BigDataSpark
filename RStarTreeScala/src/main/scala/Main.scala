import Geometry.Point
import TreeFunctions.RStarTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  private def readPointsRDD(sc: SparkContext, dataPath: String): (RDD[Point], Int) = {
    val pointsRDD = sc.textFile(dataPath)
      .mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter}
      .map(line => line.split(",").map(_.trim.toDouble))
      .map(array => new Point(array))
      .repartition(1)

    val nDims = pointsRDD.take(1)(0).nDims

    pointsRDD.take(10).foreach{p => println(s"${p.nDims} ${p.serialize}")}
    (pointsRDD, nDims)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SkylineRStarTree")
    val sc = new SparkContext(conf)

    val dataPath = "file:///C:/Users/karal/progr/Scala/BigDataSpark/dist_generator/uniform.csv"
    val (pointsRDD, nDims) = readPointsRDD(sc, dataPath)

    val rTreePerPartition: RDD[RStarTree] = pointsRDD.mapPartitions { partition =>
      Iterator(new RStarTree(partition, nDims))
    }
    rTreePerPartition.zipWithIndex.foreach { case (rTree, rTreeID) => rTree.createTree(rTreeID) }

    sc.stop()
  }


}