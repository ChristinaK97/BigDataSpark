package Util

import Geometry.Point
import org.apache.hadoop.shaded.com.google.gson.{Gson, GsonBuilder}

import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class ExecutionStats(
    partitionsResults: Array[( // For each partition:
          String,                         // partID
          ListBuffer[Point],              // Skyline Points
          mutable.PriorityQueue[Point],   // Partition's Top k
          mutable.PriorityQueue[Point],   // Partition's Skyline Top k
          (Long, Long),                   // CreateTree (start, end)
          List[(Long, Long)],             // 3 types of queries (start, end)
          Int,                            // IOs
          Int                             // nOverflow
    )],
    aggrTimes: (Long, Long, Long, Long),
    dataPath: String,
    dataSize: Long,
    nDims: Int,
    nPartitions: Int,
    kForDataset: Int,
    kForSkyline: Int
) {

  val distribution: String = dataPath.substring(dataPath.lastIndexOf("/")+1, dataPath.indexOf("."))

  val ((totalTreeCreationTime, totalSkylineTime, totalTopKTime, totalSkyTopKTime_Sol1, totalSkyTopKTime_Sol2),
       (aggrSkylineTime, aggrTopKTime, aggrSkyTopKTime)) = calculateTimes()

  val (totalIOs, totalNOverflow) = calculateOperationsSum()

  writeToJSON()

  private def calculateTimes(): ((Double,Double,Double,Double,Double), (Double,Double,Double)) = {
    val treeCreationTimes: Array[(Long,Long)] = partitionsResults.map(_._5)
    val totalTreeCreationTime = globalElapsedTime(treeCreationTimes).toDouble / 1e9

    val queriesTimes: Array[List[(Long,Long)]] = partitionsResults.map(_._6)

    val skylineTimes = ListBuffer[(Long, Long)]()
    val topKTimes = ListBuffer[(Long, Long)]()
    val skyTopKTimes_Sol1 = ListBuffer[(Long, Long)]()

    queriesTimes.map{ partitionTimes: List[(Long, Long)] =>
      skylineTimes += partitionTimes.head
      topKTimes += partitionTimes(1)
      skyTopKTimes_Sol1 += partitionTimes(2)
    }

    val (aggrSkylineTime, aggrTopKTime, aggrSkyTopKTime_Sol1, totalSkyTopKTime_Sol2L) = aggrTimes
    val totalSkyTopKTime_Sol2 = totalSkyTopKTime_Sol2L.toDouble / 1e9

    val totalSkylineTime = globalQueryTime(skylineTimes, aggrSkylineTime).toDouble / 1e9
    val totalTopKTime    = globalQueryTime(topKTimes, aggrTopKTime).toDouble / 1e9
    val totalSkyTopKTime_Sol1 = globalQueryTime(skyTopKTimes_Sol1, aggrSkyTopKTime_Sol1).toDouble / 1e9

    println(s"\nTotal aR*Trees creation time =\t $totalTreeCreationTime" +
            s"\nTotal skyline time =\t $totalSkylineTime" +
            s"\nTotal Top-k time =\t $totalTopKTime" +
            s"\nTotal Skyline Top-k time solution 1 (SCG-based) =\t $totalSkyTopKTime_Sol1" +
            s"\nTotal Skyline Top-k time solution 2 (PointCount-based) =\t $totalSkyTopKTime_Sol2")

    ((totalTreeCreationTime, totalSkylineTime, totalTopKTime, totalSkyTopKTime_Sol1, totalSkyTopKTime_Sol2),
      (aggrSkylineTime.toDouble / 1e9, aggrTopKTime.toDouble / 1e9, aggrSkyTopKTime_Sol1.toDouble / 1e9))
  }


  private def calculateOperationsSum(): (Int, Int) = {
    (partitionsResults.map(_._7).sum ,
     partitionsResults.map(_._8).sum )
  }


  private def globalQueryTime(qPartsTimes: ListBuffer[(Long,Long)], aggrTime: Long): Long ={
    val totalQueryTime = globalElapsedTime(qPartsTimes) + aggrTime
    totalQueryTime
  }

  private def globalElapsedTime(partitionsTimes: Iterable[(Long, Long)]) : Long = {
    val minStartTime = partitionsTimes.minBy(_._1)._1
    val maxEndTime   = partitionsTimes.maxBy(_._2)._2
    maxEndTime - minStartTime
  }

  private def writeToJSON(): Unit = {
    val jsonFile = s"${distribution}_N${dataSize}_D${nDims}_P${nPartitions}_Dk${kForDataset}_Sk${kForSkyline}.json"
    val jsonPath = Paths.get(s"Results/$jsonFile")
    Files.createDirectories(jsonPath.getParent) // Create parent directories if not exists
    val result = ExperimentResults(
      distribution,
      dataSize,
      nDims,
      nPartitions,
      totalTreeCreationTime,
      totalIOs,
      totalNOverflow,
      totalSkylineTime,
      aggrSkylineTime,
      kForDataset,
      totalTopKTime,
      aggrTopKTime,
      kForSkyline,
      totalSkyTopKTime_Sol1,
      aggrSkyTopKTime,
      totalSkyTopKTime_Sol2
    )

    val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
    val jsonResults: String = gson.toJson(result)
    Files.write(jsonPath, jsonResults.getBytes)
  }



}
