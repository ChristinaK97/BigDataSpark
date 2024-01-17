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

    val datasetSkyline = mergeSkylineResults(skylines)
    val datasetTopK = mergeTopKResults(partitionsTopK, kForDataset, null)
    val skylineTopK = mergeTopKResults(skylinesTopK, kForSkyline, datasetSkyline)

    printResults(datasetSkyline, datasetTopK, skylineTopK)
  }

  private def mergeSkylineResults(skylines: Array[ListBuffer[Point]]): ListBuffer[Point] = {
    val skylineRTree = new RStarTree(skylines.flatten.iterator, nDims)
    skylineRTree.createTree("skyline")
    val datasetSkyline = skylineRTree.runSkylineQuery()
    skylineRTree.close()
    datasetSkyline
  }



  // TODO : duplicate count
  private def mergeTopKResults(pTopKQueues: Array[mutable.PriorityQueue[Point]],
                               k: Int,
                               datasetSkyline: ListBuffer[Point])
  : mutable.PriorityQueue[Point] = {

    var pTopK: Array[Array[Point]] = pTopKQueues.map(queue => queue.toArray)

    if(datasetSkyline != null) { // Για εύρεση topK μόνο από το skyline (Q3)
      // Κράτα μόνο τα σημεία που ανήκουν στο τελικό skyline από όλο το dataset
      val skylinePointsIDs: Set[Int] = datasetSkyline.map(p => p.getPointID).toSet
      pTopK = pTopK.map(partitionTopK => partitionTopK.filter(p => skylinePointsIDs.contains(p.getPointID)))
    }

    for(i <- 0 until nPartitions) {
      for(j <- i+1 until nPartitions) {
        for {
          p_i <- pTopK(i)
          p_j <- pTopK(j) }{

            if( p_i.dominates(p_j) )
              p_i.increaseDomScore(p_j.getDomScore + 1)
            else if ( p_j.dominates(p_i) )
              p_j.increaseDomScore(p_i.getDomScore + 1)
      }
    }}


    implicit val topKOrdering: Ordering[Point] = Ordering.by(point => - point.getDomScore)
    val mergedTopK: mutable.PriorityQueue[Point] = mutable.PriorityQueue.empty[Point](topKOrdering)

    for {
        partitionTopK: Array[Point] <- pTopK
        p: Point <- partitionTopK }{

        if (mergedTopK.size < k)
          mergedTopK.enqueue(p)
        else if (topKOrdering.compare(p, mergedTopK.head) < 0) {
          mergedTopK.dequeue()
          mergedTopK.enqueue(p)
        }
    }
    mergedTopK
  }








  // ===================================================================================================================

  def printResults(sky: ListBuffer[Point], datasetMergedTopK: mutable.PriorityQueue[Point], skyMergedTopK: mutable.PriorityQueue[Point]): Unit = {
    println(skylineToString(sky))
    println(topKToString(datasetMergedTopK, fromSkyline = false))
    println(topKToString(skyMergedTopK, fromSkyline = true))
  }

  def skylineToString(sky: ListBuffer[Point]): String = {
    val sb: StringBuilder = new StringBuilder()
    sb.append(s"\nSkyline Results :\n\t# points = ${sky.length}\n\t")
    sky.foreach { p: Point =>
      sb.append(p.serialize)
      sb.append("\n\t")
    }
    sb.toString()
  }

  def topKToString(mergedTopK: mutable.PriorityQueue[Point], fromSkyline: Boolean): String = {
    val sb = new StringBuilder()
    sb.append(s"\n${if(fromSkyline)"Skyline" else "Dataset"} Top K Results :\n\t# points = ${mergedTopK.size}\n\t")
    val topKCopy = mergedTopK.clone()
    while (topKCopy.nonEmpty) {
      val p = topKCopy.dequeue()
      sb.append(s"\t${p.toString}\tDom Score = ${p.getDomScore}\n")
    }
    sb.toString()
  }


}
