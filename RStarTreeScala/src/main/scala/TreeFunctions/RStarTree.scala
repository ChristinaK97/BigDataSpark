package TreeFunctions

import FileHandler.IndexFile
import Geometry.{Point, Rectangle}
import TreeFunctions.CreateTreeFunctions.CreateTree
import TreeFunctions.Queries.{SkylineBBS, SkylineTopK, TopK}
import TreeStructure.TreeNode
import Util.Constants.{DEBUG, DEBUG_SKY, N, RESET_TREES}
import Util.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class RStarTree(pointsPartition: Iterator[Point], nDims: Int) {
  N = nDims
  private var indexFile: IndexFile = _
  private var logger: Logger = _


  def createTree(rTreeID: Long) : Unit = {
    indexFile = new IndexFile(RESET_TREES, rTreeID)
    logger = new Logger(indexFile.PARTITION_DIR)
    if(indexFile.getTreeWasReset)
      new CreateTree(indexFile, pointsPartition, logger)
    if(DEBUG) validateDataConsistency()
  }


/* Ερωτήματα: Υπολογισμός skyline στο dataset και top k ------------------------------------------------------------- */

  def runQueries(kForDataset: Int, kForSkyline: Int): (ListBuffer[Point], mutable.PriorityQueue[Point], mutable.PriorityQueue[Point])  = {
    // partition skyline
    logger.info("-"*100 + "\nCompute Skyline\n" + "-"*100)
    val skyline = new SkylineBBS(indexFile, logger).BranchAndBound()

    // top k from skyline points
    logger.info("-"*100 + s"\nCompute Skyline Top${kForSkyline}\n" + "-"*100)
    val topKSkyline = new SkylineTopK(indexFile, skyline, kForSkyline, logger).SimpleCountGuidedAlgorithm()

    // top k from the whole partition
    logger.info("-"*100 + s"\nCompute Dataset Top${kForDataset}\n" + "-"*100)
    val topKPartition = new TopK(indexFile, kForDataset, logger).SimpleCountGuidedAlgorithm()
    (
      skyline.map{case (p,_) => p},
      topKPartition,
      topKSkyline
    )
  }


/* -------------------------------------------------------------------------------------------------------------------*/
  def close(): Unit = {
    logger.close()
    indexFile.closeFile()
  }









/* ---------------------------------------------------------------------------------------------------------*/
/* FOR TESTING */
/* ---------------------------------------------------------------------------------------------------------*/
  private def validateDataConsistency(): Unit = {
    searchForMissingPoints()
    validateAggrCounters()
    logger.info("FINISHED CHECK")
  }

  private def searchForMissingPoints() : Unit = {
    val counter = mutable.HashMap[Int, ListBuffer[Int]]()
    for (nodeID <- 1 until indexFile.getNumOfNodes + 1) {
      val node: TreeNode = indexFile.retrieveNode(nodeID)
      if (node.isLeaf)
        node.foreach { case p: Point =>
          if(counter.contains(p.getPointID))
            counter(p.getPointID) += nodeID
          else
            counter(p.getPointID) = ListBuffer[Int](nodeID)
        }
    }
    //logger.info("-" * 100 + "\nPoints in leaves :\n")
    //for (pID <- counter.indices) logger.info(s"Point $pID in # leaves = ${counter(pID).length} : ${counter(pID)}")
    logger.info("-" * 100 + "\nMissing points:\n")
    counter.foreach{case (pID, pLeaves) =>
      if (pLeaves.length > 1)
        logger.info(s"Point $pID : Found Duplicates (# = ${pLeaves.length}) in leaves = ${pLeaves.toString()}")
      else if (pLeaves.isEmpty)
        logger.info(s"Point $pID : Not found in leaves")
    }
  }

  private def validateAggrCounters(): Unit = {
    logger.info("\nAggregation (count) inconsistencies:\n")

    val rootNode = indexFile.retrieveNode(indexFile.getRootID)
    if(rootNode.getSCount != indexFile.getNumOfPoints)
      logger.info(s"Root [${rootNode.getNodeID}].SCount = ${rootNode.getSCount} isDiffThan total # points in dataset = ${indexFile.getNumOfPoints}\t\t ${rootNode.serialize}")

    for (nodeID <- 1 until indexFile.getNumOfNodes + 1) {
      val node = indexFile.retrieveNode(nodeID)
      val entriesCountSum = node.getEntries.map(entry => entry.getCount).sum

      if(node.getSCount != entriesCountSum)
        logger.info(s"Node [$nodeID].SCount = ${node.getSCount} isDiffThan sum of entries' count = $entriesCountSum \t " +
          s"isLeaf ${node.isLeaf} \t#entries = ${node.getNumberOfEntries} : \t" +
          s"${node.getEntries.map(e => if(node.isLeaf) e.asInstanceOf[Point].getPointID else e.asInstanceOf[Rectangle].getChildID)}" +
          s" \t\t ${node.serialize}")

      if(!node.isLeaf)
        node.zipWithIndex.foreach{case (mbr, i) =>
          val childNode = indexFile.retrieveNode(mbr.asInstanceOf[Rectangle].getChildID)
          if(mbr.getCount != childNode.getSCount)
            logger.info(s"MBR [$nodeID][$i].count = ${mbr.getCount} isDiffThan [${childNode.getNodeID}].SCount = ${childNode.getSCount}" +
              s"\t\tMBR = $mbr \t\t childNode = ${childNode.serialize}")
        }
      else if(node.getNumberOfEntries != node.getSCount)
        logger.info(s"Leaf node [${node.getNodeID}].SCount = ${node.getSCount} isDiffThan # point entries = ${node.getNumberOfEntries}")
    }
  }



}
