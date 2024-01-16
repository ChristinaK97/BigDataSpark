package TreeFunctions.Queries

import Geometry.Point
import TreeFunctions.RStarTree

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MergePartitionsResults(
  nDims: Int,            // Skyline Points,    Partitions Top k,              Skylines Top k
  partitionsResults: Array[(ListBuffer[Point], mutable.PriorityQueue[Point], mutable.PriorityQueue[Point])],
  kForDataset: Int,
  kForSkyline: Int
){

  private val nPartitions = partitionsResults.length
  mergeResults()

  private def mergeResults(): Unit = {
    val skylines:       Array[ListBuffer[Point]] = partitionsResults.map(_._1)
    val partitionsTopK: Array[mutable.PriorityQueue[Point]] = partitionsResults.map(_._2)
    val skylinesTopK:   Array[mutable.PriorityQueue[Point]] = partitionsResults.map(_._3)

    val datasetSkyline = calculateDatasetSkyline(skylines)

  }

  private def calculateDatasetSkyline(skylines: Array[ListBuffer[Point]]): ListBuffer[Point] = {
    val skylineRTree = new RStarTree(skylines.flatten.iterator, nDims)
    skylineRTree.createTree("skyline")
    val datasetSkyline = skylineRTree.runSkylineQuery()
    skylineRTree.close()
    datasetSkyline
  }

}
