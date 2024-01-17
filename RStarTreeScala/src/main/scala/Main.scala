import Geometry.Point
import TreeFunctions.Queries.MergePartitionsResults
import TreeFunctions.RStarTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  private def readPointsRDD(sc: SparkContext, dataPath: String): (RDD[Point], Int) = {

    val pointsRDD = sc.textFile(dataPath)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter}
      .zipWithIndex()
      .map { case (line, id) =>
        val coordinates = line.split(",").map(_.trim.toDouble)
        new Point(id.toInt, coordinates)
      }
    val nDims = pointsRDD.take(1)(0).nDims

    //pointsRDD.take(10).foreach{p => println(s"${p.nDims} ${p.serialize}")}
    (pointsRDD, nDims)
  }


  def main(args: Array[String]): Unit = {
    //ARGS:
    val dataPath = "file:///C:/Users/karal/progr/Scala/BigDataSpark/dist_generator/uniform.csv"
    val nPartitions = 4
    val kForDataset = 10
    val kForSkyline = 10

    val conf = new SparkConf().setMaster("local[*]").setAppName("RStarTreeScala")
    val sc = new SparkContext(conf)

    val (pointsRDD, nDims) = readPointsRDD(sc, dataPath)
    val repartitionedRDD = pointsRDD.repartition(nPartitions)
    println(s"# partitions = ${repartitionedRDD.getNumPartitions}")


    val partitionsResults = repartitionedRDD.mapPartitionsWithIndex { (partitionID, pointsPartition) =>
      val rTree = new RStarTree(pointsPartition, nDims)
      rTree.createTree(partitionID.toString)
      val (skyline, topKPartition, topKSkyline)  = rTree.runQueries(kForDataset, kForSkyline)
      rTree.close()
      Iterator((partitionID.toString, skyline, topKPartition, topKSkyline))
    }.collect()

    new MergePartitionsResults(nDims, partitionsResults, kForDataset, kForSkyline)

    sc.stop()
  }


}