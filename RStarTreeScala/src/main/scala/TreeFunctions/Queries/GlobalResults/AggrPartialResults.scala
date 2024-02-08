package TreeFunctions.Queries.GlobalResults

import FileHandler.IndexFile
import Geometry.Point
import TreeFunctions.RStarTree
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class AggrPartialResults(
  sc: SparkContext,
  distributedAggregation: Boolean,
  nDims: Int,
  partitionsResults: Array[(  // For each partition:
      String,                       // partID
      ListBuffer[Point],            // Skyline Points
      mutable.PriorityQueue[Point], // Partition's Top k
      mutable.PriorityQueue[Point], // Partition's Skyline Top k
      (Long, Long),                 // CreateTree (start, end)
      List[(Long, Long)],           // 3 types of queries (start, end)
      Int,                          // IOs
      Int                           // nOverflow
  )],
  kForDataset: Int,
  kForSkyline: Int
){

  private val nPartitions = partitionsResults.length                                                                    ; println("Executors completed")
  private var partitionIDs: Array[String] = _


  def aggregateResults(): (Long, Long, Long, Long) = {
    partitionIDs = partitionsResults.map(_._1)
    val skylines:       Array[ListBuffer[Point]] = partitionsResults.map(_._2)
    val partitionsTopK: Array[mutable.PriorityQueue[Point]] = partitionsResults.map(_._3)
    val skylinesTopK:   Array[mutable.PriorityQueue[Point]] = partitionsResults.map(_._4)


    var start = System.nanoTime()                                                                                       ; println("Aggregating skyline...")
    /*Q1*/val datasetSkyline = aggregateSkylineResults(skylines)
    var end = System.nanoTime()
    val aggrSkylineTime = end - start

    start = System.nanoTime()                                                                                           ; println("Aggregating top-k...")
    /*Q2*/val datasetTopK = aggregateTopKResults(partitionsTopK, kForDataset, null)
    end = System.nanoTime()
    val aggrTopKTime = end- start


    start = System.nanoTime()                                                                                           ; println("Aggregating skyline top-k solution 1...")
    /*Q3-Sol 1*/ val skylineTopK_Sol1 = aggregateTopKResults(skylinesTopK, kForSkyline, datasetSkyline)
    end = System.nanoTime()
    val aggrSkyTopKTime_Sol1 = end - start

    start = System.nanoTime()                                                                                           ; println("Aggregating skyline top-k solution 2...")
    /*Q3-Sol 2*/ val skylineTopK_Sol2 = Q3PointCountBasedSolution(datasetSkyline)
    end = System.nanoTime()
    val totalSkyTopKTime_Sol2 = end - start

    printResults(datasetSkyline, datasetTopK, skylineTopK_Sol1, skylineTopK_Sol2)
    (aggrSkylineTime, aggrTopKTime, aggrSkyTopKTime_Sol1, totalSkyTopKTime_Sol2)
  }



// AGGREGATE SKYLINE RESULTS -------------------------------------------------------------------------------------------

  private def aggregateSkylineResults(skylines: Array[ListBuffer[Point]]): ListBuffer[Point] = {
    val skylineRTree = new RStarTree(skylines.flatten.iterator, nDims)
    skylineRTree.createTree("skyline")
    val datasetSkyline = skylineRTree.runSkylineQuery()
    skylineRTree.close()
    datasetSkyline
  }


// AGGREGATE TOP K RESULTS ---------------------------------------------------------------------------------------------

  /** Q3_Sol2 [Step 1]
   * Εκτελεί την 2η προσέγγιση για το ερώτημα Q3, που χρησιμοποιεί μόνο τον Point Count
   * αλγόριθμο και καθόλου τον SCG. */
  private def Q3PointCountBasedSolution(datasetSkyline: ListBuffer[Point]): mutable.PriorityQueue[Point] = {

    // only the points that belong to the global skyline are considered as candidates
    val skylinePoints: Array[(String, Point)] =
      datasetSkyline.map(p => ("-1", p)).toArray

    // global dom score for each point in all partitions
    val aggregatedSum = calculateGlobalDomScores(skylinePoints)
    datasetSkyline.foreach { p =>
      p.setDomScore(aggregatedSum.getOrElse(p.getPointID, 0))
    }
    // global top k set from the global skyline
    globalTopKHeap(
      Array(datasetSkyline.toArray),
      kForSkyline
    )
  }



  /** Q2 & Q3_Sol1 [Step 1]
   *  Εκτελεί το aggregation step για τα topK queries ie, βρίσκει το global topK set από τα local
   *  AGGREGATION FOR
   *      Q2: GLOBAL DATASET TOP-K FROM PARTIAL (PARTITION) TOP-K SETS
   *      Q3: GLOBAL SKYLINE TOP-K FROM PARTIAL (PARTITION SKYLINE) TOP-K
   *          Εκτελεί την 1η προσεγγίση (SCG-based: modified SCG alg + aggr step)
   *
   *  To aggregation step μπορεί να γίνει μόνο αποκλειστικά από τον driver (centralized)
   *  ή σε συνεργασία με τους executors (decentralized)
   *
   *  @param datasetSkyline : != null για το Q3
   */
  private def aggregateTopKResults(pTopKQueues: Array[mutable.PriorityQueue[Point]],
                               k: Int,
                               datasetSkyline: ListBuffer[Point])
  : mutable.PriorityQueue[Point] = {

    var pTopK: Array[Array[Point]] = pTopKQueues.map(queue => queue.toArray)

    if(datasetSkyline != null) { // Για εύρεση topK μόνο από το skyline (Q3)
      // Κράτα μόνο τα σημεία που ανήκουν στο τελικό skyline από όλο το dataset
      val skylinePointsIDs: Set[Int] = datasetSkyline.map(p => p.getPointID).toSet
      pTopK = pTopK.map(partitionTopK => partitionTopK.filter(p => skylinePointsIDs.contains(p.getPointID)))
    }

    if(distributedAggregation) distributedGlobalTopKScores(pTopK)
    else centralizedGlobalTopKScores(pTopK)

    globalTopKHeap(pTopK, k)
  }


  /** Q2 & Q3_Sol1 [Step 2-Option 1]
   * Το aggregation step για τα topK queries εκτελείται αποκλειστικά από τον driver */
  private def centralizedGlobalTopKScores(pTopK: Array[Array[Point]]): Unit = {

    val indexFiles: Map[String, IndexFile] = partitionIDs.map(partitionID =>
      partitionID -> new IndexFile(false, partitionID)
    ).toMap

    pTopK.zipWithIndex.foreach{case (partition_i_topK, i) =>
      val partition_i_id = partitionIDs(i)

      indexFiles.foreach{case (partition_j_id, indexFile_j) =>
        if(!partition_i_id.equals(partition_j_id))
          new PointCount(partition_i_topK, indexFile_j)
      }
    }
  }


  /** Q2 & Q3_Sol1 [Step 2-Option 2]
   *  Το aggregation step για τα topK queries εκτελείται από τον driver και τους executors
   *  1. Οι executors θα υπολογίσουν για κάθε point που δεν ανήκει στο δικό τους partition το dom score
   *  2. Ο driver θα αθροίσει τα επιμέρους αποτελέσματα για να βρει το global dom score
   */
  private def distributedGlobalTopKScores(pTopK: Array[Array[Point]]): Unit = {

    val allTopK: Array[(String, Point)] =
      pTopK.zipWithIndex.flatMap{ case (partition_i_topK, i) =>
        val partition_i_id = partitionIDs(i)
        partition_i_topK.map(point => partition_i_id -> point)
      }

    val aggregatedSum = calculateGlobalDomScores(allTopK)

    pTopK.foreach { partitionTopK =>
      partitionTopK.foreach { p =>
        p.increaseDomScore(aggregatedSum.getOrElse(p.getPointID, 0))
      }
    }
  }



  /** Q2 & Q3_Sol1 [Step 3] , Q3_Sol2 [Step 2]
   *  Υπολογίζει για κάθε point που είναι candidate για το global top-k result,
   *  το άθροισμα των partial dom scores στα επιμέρους partitions του dataset
   *  @param points Τα candidate points. String είναι το partitionID που ανήκει
   *                το σημείο. Αν το σημείο ανήκει στο part i, τότε στο άθροισμα
   *                θα συμμετέχουν τα partial dom scores από κάθε partition j != i
   *                Αλλιώς, partitionID == "-1", τότε το άθροισμα θα είναι το global
   *                dom score από όλα τα partitions (Q3 - solution 2)
   * @return Map[pointID : Int, dom score sum: Int == Πλήθος σημείων που κυριαρχούνται από
   *                  το point σε κάθε partition που δεν έχει ίδιο id με το partitionID που
   *                  δόθηκε για το point, μπορεί να είναι και το global]
   */
  private def calculateGlobalDomScores(points: Array[(String, Point)]): Map[Int, Int] = {
    val pointsBroadcast: Broadcast[Array[(String, Point)]] = sc.broadcast(points)
    val partitionIDsRDD = sc.parallelize(partitionIDs, nPartitions)

    val partialSumRDD = partitionIDsRDD.mapPartitions(iter => {
      val partitionID: String = iter.next()
      val indexFile = new IndexFile(false, partitionID)
      Iterator(
        new PointCount(pointsBroadcast.value, indexFile).partialSum.asInstanceOf[Map[Int, Int]]
      )
    })

    val aggregatedSum = partialSumRDD.reduce { (sum1, sum2) =>
      (sum1.keySet ++ sum2.keySet).map { pointId =>
        pointId -> (sum1.getOrElse(pointId, 0) + sum2.getOrElse(pointId, 0))
      }.toMap
    }
    aggregatedSum
  }





  /** Q2 & Q3_Sol1 [Step 4] , Q3_Sol2 [Step 3]
   *  Βρίσκει το global topK φτιάχνοντας το topK heap */
  private def globalTopKHeap(points: Array[Array[Point]], k: Int): mutable.PriorityQueue[Point] = {
    implicit val topKOrdering: Ordering[Point] = Ordering.by(point => -point.getDomScore)
    val aggrTopK: mutable.PriorityQueue[Point] = mutable.PriorityQueue.empty[Point](topKOrdering)

    for {
      partitionTopK: Array[Point] <- points
      p: Point <- partitionTopK} {

      if (aggrTopK.size < k)
        aggrTopK.enqueue(p)
      else if (topKOrdering.compare(p, aggrTopK.head) < 0) {
        aggrTopK.dequeue()
        aggrTopK.enqueue(p)
      }
    }
    aggrTopK
  }









// =====================================================================================================================

  def printResults(sky: ListBuffer[Point], datasetAggrdTopK: mutable.PriorityQueue[Point], skyAggrdTopK1: mutable.PriorityQueue[Point], skyAggrdTopK2: mutable.PriorityQueue[Point]): Unit = {
    println(skylineToString(sky))
    println(topKToString(datasetAggrdTopK, fromSkyline = false))
    println(topKToString(skyAggrdTopK1, fromSkyline = true))
    println(topKToString(skyAggrdTopK2, fromSkyline = true))
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

  def topKToString(AggrdTopK: mutable.PriorityQueue[Point], fromSkyline: Boolean): String = {
    val sb = new StringBuilder()
    sb.append(s"\n${if(fromSkyline)"Skyline" else "Dataset"} Top K Results :\n\t# points = ${AggrdTopK.size}\n")
    val topKCopy = AggrdTopK.clone()
    while (topKCopy.nonEmpty) {
      val p = topKCopy.dequeue()
      sb.append(s"\t${p.toString}\tDom Score = ${p.getDomScore}\n")
    }
    sb.toString()
  }


}


