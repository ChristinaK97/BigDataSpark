package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.{LeafNode, TreeNode}
import Util.Constants.DEBUG_TOPK
import Util.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SkylineTopK(indexFile: IndexFile, sky: ListBuffer[(Point, Int)], k: Int, logger: Logger)
  extends TopK(indexFile, k, logger) {

  private val entriesWithSkyPoints = mutable.HashMap[Int, mutable.HashSet[Int]]()
  private val skyPointsIDs = mutable.HashSet[Int]()

  findEntriesContainingSkyPoints()

// ---------------------------------------------------------------------------------------------------------------------

  private def findEntriesContainingSkyPoints(): Unit = {
    val pathForLeafWasCalculated = mutable.HashSet[Int]()
    sky.foreach{case (p: Point, leafID: Int) =>

      skyPointsIDs.add(p.getPointID)
      if(!pathForLeafWasCalculated.contains(leafID)) {
        pathForLeafWasCalculated.add(leafID)
        val path = travelTree(p, leafID, ListBuffer[(Int,Int)](), root)                                                                       //logger.info(s"psky = ${p.serialize}\t$path")
        path.foreach{case (nodeID, entryIndex) =>
          if(entriesWithSkyPoints.contains(nodeID))
            entriesWithSkyPoints(nodeID) += entryIndex
          else
            entriesWithSkyPoints(nodeID) = mutable.HashSet(entryIndex)
        }//end foreach entry in the path
      }//end if path not already calculated
    }//end foreach sky point                                                                                                                   ; logger.info(s"Skyline points ids = ${skyPointsIDs.toString}\nNodeID -> {EntryIndex} that contain skyline points = ${entriesWithSkyPoints.toString()}")
  }

  private def travelTree(p: Point, leafID: Int, path: ListBuffer[(Int, Int)], currentNode: TreeNode): ListBuffer[(Int, Int)] = {
    var pathVar = path
    if (currentNode.isLeaf)
      pathVar += ((currentNode.getNodeID, -1))
    else
      currentNode.zipWithIndex.foreach { case (r: Rectangle, rIndex: Int) =>
        if (r.contains(p)) {
          pathVar += ((currentNode.getNodeID, rIndex))
          pathVar = travelTree(p, leafID, pathVar, indexFile.retrieveNode(r.getChildID))
          if (pathVar.last._1 == leafID)
            return pathVar
        }
      }//end foreach rectangle in NonLeafNode
    pathVar
  }

// ---------------------------------------------------------------------------------------------------------------------

  override def setK(): Int =
    k.min(sky.size)


  override def updateTravelMaxHeap(node: TreeNode): Unit = {
    if(entriesWithSkyPoints.contains(node.getNodeID)) {
      node.zipWithIndex.foreach{case (r: Rectangle, entryIdx: Int) =>
        if(entriesWithSkyPoints(node.getNodeID).contains(entryIdx))
          travelMaxHeap += r
      }
    }
  }

  override def updateTopKMinHeap(node: LeafNode): Unit = {                                                               ; if(DEBUG_TOPK) logger.info(s"\tUpdate topK with [${node.getNodeID}]\t # entries = ${node.getNumberOfEntries}")
    node.foreach { case p: Point =>
      if(skyPointsIDs.contains(p.getPointID))
        addPointToTopKMinHeap(p)
    }
    kthScore = topKMinHeap.head.getDomScore
  }


// BATCH COUNT ---------------------------------------------------------------------------------------------------------

  override def hasPartialDom(r: Rectangle, batch: TreeNode): Boolean = {
    batch.zipWithIndex.foreach { case (batchEntry, batchEntryInx) =>

      if (isRelevant(batch.getNodeID, batchEntry, batchEntryInx) &&
          !batchEntry.dominates(r.get_pm) &&
           batchEntry.dominates(r.get_pM))
              return true
    }
    false
  }


  override def increaseDomScores(batch: TreeNode, nodeEntry: GeometricObject): Unit = {
    batch.zipWithIndex.foreach { case (batchEntry, batchEntryInx) =>
      if (isRelevant(batch.getNodeID, batchEntry, batchEntryInx)
          && batchEntry.dominates(nodeEntry))
             batchEntry.increaseDomScore(nodeEntry.getCount)
    }
  }


  private def isRelevant(batchID: Int, batchEntry: GeometricObject, batchEntryIdx: Int): Boolean = {
    entriesWithSkyPoints.contains(batchID) &&
    (
      (batchEntry.isInstanceOf[Point] && skyPointsIDs.contains(batchEntry.asInstanceOf[Point].getPointID))
      || entriesWithSkyPoints(batchID).contains(batchEntryIdx)
    )
  }






//======================================================================================================================
  private def printPathForSkyPoint(p: Point, leafID: Int, path: ListBuffer[(Int,Int)]): Unit = {
    logger.info(s"Sky point : ${p.serialize}\tin leaf [$leafID]\n\t$path")
    path.foreach { case (nodeID, entryIndex) =>
      val pathNode = indexFile.retrieveNode(nodeID)
      val out = if (entryIndex != -1) pathNode.getEntry(entryIndex).serialize else pathNode.serialize
      logger.info(s"\t\t$out")
    }
  }

}
