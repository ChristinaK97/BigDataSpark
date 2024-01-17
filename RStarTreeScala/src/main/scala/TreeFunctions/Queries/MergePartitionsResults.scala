package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.Point
import TreeFunctions.RStarTree
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MergePartitionsResults(
  //sc: SparkContext,
  nDims: Int,             //partID, Skyline Points,    Partitions Top k,              Skylines Top k
  partitionsResults: Array[(String, ListBuffer[Point], mutable.PriorityQueue[Point], mutable.PriorityQueue[Point])],
  kForDataset: Int,
  kForSkyline: Int
){

  private val nPartitions = partitionsResults.length
  private var partitionIDs: Array[String] = _
  mergeResults()

  private def mergeResults(): Unit = {
    partitionIDs = partitionsResults.map(_._1)
    val skylines:       Array[ListBuffer[Point]] = partitionsResults.map(_._2)
    val partitionsTopK: Array[mutable.PriorityQueue[Point]] = partitionsResults.map(_._3)
    val skylinesTopK:   Array[mutable.PriorityQueue[Point]] = partitionsResults.map(_._4)

    val datasetSkyline = mergeSkylineResults(skylines)
    val datasetTopK = mergeTopKResults(partitionsTopK, kForDataset, null)
    val skylineTopK = mergeTopKResults(skylinesTopK, kForSkyline, datasetSkyline)

    printResults(datasetSkyline, datasetTopK, skylineTopK)
  }

// MERGE SKYLINE RESULTS -----------------------------------------------------------------------------------------------

  private def mergeSkylineResults(skylines: Array[ListBuffer[Point]]): ListBuffer[Point] = {
    val skylineRTree = new RStarTree(skylines.flatten.iterator, nDims)
    skylineRTree.createTree("skyline")
    val datasetSkyline = skylineRTree.runSkylineQuery()
    skylineRTree.close()
    datasetSkyline
  }

// MERGE TOP K RESULTS -------------------------------------------------------------------------------------------------

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

    //decentralizedGlobalTopKScores(pTopK)
    centralizedGlobalTopKScores(pTopK)
    globalTopKHeap(pTopK, k)

  }


  private def centralizedGlobalTopKScores(pTopK: Array[Array[Point]]): Unit = {

    val indexFiles: Map[String, IndexFile] = partitionIDs.map(partitionID =>
      partitionID -> new IndexFile(false, partitionID)
    ).toMap

    pTopK.zipWithIndex.foreach{case (partition_i_topK, i) =>
      val partition_i_id = partitionIDs(i)

      indexFiles.foreach{case (partition_j_id, indexFile_j) =>
        if(!partition_i_id.equals(partition_j_id))
          new MergePointCount(partition_i_topK, indexFile_j)
      }
    }
  }



  private def globalTopKHeap(pTopK: Array[Array[Point]], k: Int): mutable.PriorityQueue[Point] = {
    implicit val topKOrdering: Ordering[Point] = Ordering.by(point => -point.getDomScore)
    val mergedTopK: mutable.PriorityQueue[Point] = mutable.PriorityQueue.empty[Point](topKOrdering)

    for {
      partitionTopK: Array[Point] <- pTopK
      p: Point <- partitionTopK} {

      if (mergedTopK.size < k)
        mergedTopK.enqueue(p)
      else if (topKOrdering.compare(p, mergedTopK.head) < 0) {
        mergedTopK.dequeue()
        mergedTopK.enqueue(p)
      }
    }
    mergedTopK
  }



  /*private def decentralizedGlobalTopKScores(pTopK: Array[Array[Point]]): Unit = {
    val indexFiles: Map[String, IndexFile] = partitionIDs.map(partitionID =>
      partitionID -> new IndexFile(false, partitionID)
    ).toMap

    val dataSeq = ListBuffer[(IndexFile, ListBuffer[Point])]()
    indexFiles.foreach { case (partition_j_id, indexFile_j) =>
      val otherPartitionsPoints = ListBuffer[Point]()

      pTopK.zipWithIndex.foreach { case (partition_i_topK, i) =>
        val partition_i_id = partitionIDs(i)
        if (!partition_j_id.equals(partition_i_id))
          otherPartitionsPoints.addAll(partition_i_topK)
      }
      dataSeq += ((indexFile_j, otherPartitionsPoints))
    }
    val dataRDD: RDD[(IndexFile, ListBuffer[Point])] = sc.parallelize(dataSeq.toSeq, nPartitions)

    val partialSumsRdd = dataRDD.mapPartitions(iter => {
      iter.map { case (indexFile, points) =>
        indexFile -> new MergePointCount(points, indexFile).partialSum
      }
    })

    val groupedSumRdd = partialSumsRdd.groupByKey()

    val globalSumRdd = groupedSumRdd.mapValues(iter =>
      iter.reduce { (sum1, sum2) =>
        (sum1.keySet ++ sum2.keySet).map { pointId =>
          pointId -> (sum1.getOrElse(pointId, 0) + sum2.getOrElse(pointId, 0))
        }.toMap
      }
    )

    val result = globalSumRdd.collect()

    result.foreach { case (indexFile, combinedPartialSum) =>
      println(s"For IndexFile $indexFile:")
      combinedPartialSum.foreach { case (pointId, sum) =>
        println(s"  Point $pointId: $sum")
      }
    }
  }*/





// =====================================================================================================================

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
    sb.append(s"\n${if(fromSkyline)"Skyline" else "Dataset"} Top K Results :\n\t# points = ${mergedTopK.size}\n")
    val topKCopy = mergedTopK.clone()
    while (topKCopy.nonEmpty) {
      val p = topKCopy.dequeue()
      sb.append(s"\t${p.toString}\tDom Score = ${p.getDomScore}\n")
    }
    sb.toString()
  }


}


