import Geometry.Point
import TreeFunctions.Queries.GlobalResults.AggrPartialResults
import TreeFunctions.RStarTree
import Util.Constants.RESET_TREES
import Util.DirectoryDeletion.deleteDirectory
import Util.ExecutionStats
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

    //pointsRDD.take(10).foreach{p => println(sD${p.nDims} ${p.serialize}")}
    (pointsRDD, nDims)
  }


  def main(args: Array[String]): Unit = {

    // Declare variables with default values
    var dataPath = s"file:///C:/Users/karal/progr/Scala/BigDataSpark/dist_generator/50k/uniform/uniform_50k_2D.csv"
    var nPartitions = 2
    var kForDataset = 10
    var kForSkyline = 10

    // Check if the correct number of arguments is provided
    if (args.length == 4) {
      dataPath = args(0)
      nPartitions = args(1).toInt
      kForDataset = args(2).toInt
      kForSkyline = args(3).toInt
    }

    val distributedAggregation = true

    val conf = new SparkConf().setMaster("local[*]").setAppName("RStarTreeScala")
    val sc = new SparkContext(conf)

    if(RESET_TREES)
      deleteDirectory("RTrees")

    // Initially, the dataset is randomly partitioned into partitions that
    // are assigned to executors through the map partitions operation
    val (pointsRDD, nDims) = readPointsRDD(sc, dataPath)
    val repartitionedRDD = pointsRDD.repartition(nPartitions)

    val partitionsResults = repartitionedRDD.mapPartitionsWithIndex { (partitionID, pointsPartition) =>

      val rTree = new RStarTree(pointsPartition, nDims)
      val ((startTree, endTree), (ios, nOverflow))      = rTree.createTree(partitionID.toString)
      val (skyline, topKPartition, topKSkyline, times)  = rTree.runQueries(kForDataset, kForSkyline)
      rTree.close()

      Iterator((partitionID.toString, skyline, topKPartition, topKSkyline,
                (startTree, endTree), times,
                ios, nOverflow
      ))

    }.collect()

    // Aggregate partial results
    val aggrTimes = if(nPartitions > 1)
      new AggrPartialResults(sc, distributedAggregation, nDims, partitionsResults, kForDataset, kForSkyline)
          .aggregateResults()
    else (0L, 0L, 0L, 0L)

    // Extract execution stats
    new ExecutionStats(
      partitionsResults,
      aggrTimes,
      dataPath,
      pointsRDD.count(),
      nDims,
      nPartitions,
      kForDataset,
      kForSkyline
    )
    sc.stop()
  }


}