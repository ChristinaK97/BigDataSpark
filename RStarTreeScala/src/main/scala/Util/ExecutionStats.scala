package Util

import Geometry.Point
import org.apache.hadoop.shaded.com.google.gson.{Gson, GsonBuilder}

import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class ExecutionStats(
     partitionsResults: Array[(String, ListBuffer[Point], mutable.PriorityQueue[Point], mutable.PriorityQueue[Point],
                        //Tree(start, end), 3 types of queries (start, end)
                        (Long, Long), List[(Long, Long)])],
     mergeTimes: (Long, Long, Long),
     dataPath: String,
     dataSize: Long,
     nDims: Int,
     nPartitions: Int,
     kForDataset: Int,
     kForSkyline: Int
) {

  val distribution: String = dataPath.substring(dataPath.lastIndexOf("/")+1, dataPath.indexOf("."))
  val ((totalTreeCreationTime, totalSkylineTime, totalTopKTime, totalSkyTopKTime),
       (mergeSkylineTime, mergeTopKTime, mergeSkyTopKTime)) = calculateTimes()
  writeToJSON()

  private def calculateTimes(): ((Double,Double,Double,Double), (Double,Double,Double)) = {
    val treeCreationTimes: Array[(Long,Long)] = partitionsResults.map(_._5)
    val totalTreeCreationTime = globalElapsedTime(treeCreationTimes).toDouble / 1e9

    val queriesTimes: Array[List[(Long,Long)]] = partitionsResults.map(_._6)

    val skylineTimes = ListBuffer[(Long, Long)]()
    val topKTimes = ListBuffer[(Long, Long)]()
    val skyTopKTimes = ListBuffer[(Long, Long)]()

    queriesTimes.map{ partitionTimes: List[(Long, Long)] =>
      skylineTimes += partitionTimes.head
      topKTimes += partitionTimes(1)
      skyTopKTimes += partitionTimes(2)
    }

    val (mergeSkylineTime, mergeTopKTime, mergeSkyTopKTime) = mergeTimes

    val totalSkylineTime = globalQueryTime(skylineTimes, mergeSkylineTime).toDouble / 1e9
    val totalTopKTime    = globalQueryTime(topKTimes, mergeTopKTime).toDouble / 1e9
    val totalSkyTopKTime = globalQueryTime(skyTopKTimes, mergeSkyTopKTime).toDouble / 1e9

    println(s"\nTotal aR*Trees creation time =\t $totalTreeCreationTime" +
            s"\nTotal skyline time =\t $totalSkylineTime" +
            s"\nTotal Top-k time =\t $totalTopKTime" +
            s"\nTotal Skyline Top-k time =\t $totalSkyTopKTime")

    ((totalTreeCreationTime, totalSkylineTime, totalTopKTime, totalSkyTopKTime),
      (mergeSkylineTime.toDouble / 1e9, mergeTopKTime.toDouble / 1e9, mergeSkyTopKTime.toDouble / 1e9))
  }



  private def globalQueryTime(qPartsTimes: ListBuffer[(Long,Long)], mergeTime: Long): Long ={
    val totalQueryTime = globalElapsedTime(qPartsTimes) + mergeTime
    totalQueryTime
  }

  private def globalElapsedTime(partitionsTimes: Iterable[(Long, Long)]) : Long = {
    val minStartTime = partitionsTimes.minBy(_._1)._1
    val maxEndTime   = partitionsTimes.maxBy(_._2)._2
    maxEndTime - minStartTime
  }

  private def writeToJSON(): Unit = {
    val jsonFile = s"${distribution}_N${dataSize}_D${nDims}_P${nPartitions}_Dk${kForDataset}_Sk${kForSkyline}.json"
    val jsonPath = Paths.get(s"Results/$distribution/$jsonFile")
    Files.createDirectories(jsonPath.getParent) // Create parent directories if not exists
    val result = ExperimentResults(
      distribution,
      dataSize,
      nDims,
      nPartitions,
      totalTreeCreationTime,
      totalSkylineTime,
      mergeSkylineTime,
      kForDataset,
      totalTopKTime,
      mergeTopKTime,
      kForSkyline,
      totalSkyTopKTime,
      mergeSkyTopKTime
    )

    val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
    val jsonResults: String = gson.toJson(result)
    Files.write(jsonPath, jsonResults.getBytes)
  }



}
