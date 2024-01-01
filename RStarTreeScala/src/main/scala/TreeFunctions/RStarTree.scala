package TreeFunctions

import FileHandler.IndexFile
import Geometry.{Point, Rectangle}
import TreeFunctions.CreateTreeFunctions.CreateTree
import TreeFunctions.Queries.SkylineBBS
import TreeStructure.TreeNode
import Util.Constants.N
import Util.Logger

import scala.collection.mutable.ListBuffer

class RStarTree(pointsPartition: Iterator[Point], nDims: Int) {
  N = nDims
  private var indexfile: IndexFile = _
  private val logger = new Logger()
  private val resetTree = true


  def createTree(rTreeID: Long) : Unit = {
    indexfile = new IndexFile(resetTree, rTreeID)
    if(indexfile.getTreeWasReset)
      new CreateTree(indexfile, pointsPartition, logger)
    validateDataConsistency()
  }

/* Ερώτημα 1: Υπολογισμός skyline στο dataset ----------------------------------------------------------- */

  def computeDatasetSkyline(retrieveDomArea: Boolean): Either[ListBuffer[(Point,Rectangle)], ListBuffer[Point]] = {
    val datasetSkyline: SkylineBBS = new SkylineBBS(indexfile, logger)
    if (retrieveDomArea)
      Left(datasetSkyline.getSkylineWithDomAreas)
    else
      Right(datasetSkyline.getSkylinePoints)
  }

/* ---------------------------------------------------------------------------------------------------------*/
  def close(): Unit = {
    logger.close()
    indexfile.closeFile()
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
    val counter: Array[ListBuffer[Int]] = Array.fill[ListBuffer[Int]](indexfile.getNumOfPoints)(ListBuffer[Int]())
    for (nodeID <- 1 until indexfile.getNumOfNodes + 1) {
      val node: TreeNode = indexfile.retrieveNode(nodeID)
      if (node.isLeaf)
        node.foreach { case p: Point =>
          counter(p.getPointID) += nodeID
        }
    }
    //logger.info("-" * 100 + "\nPoints in leaves :\n")
    //for (pID <- counter.indices) logger.info(s"Point $pID in # leaves = ${counter(pID).length} : ${counter(pID)}")
    logger.info("-" * 100 + "\nMissing points:\n")
    for (pID <- counter.indices) {
      val pLeaves: ListBuffer[Int] = counter(pID)
      if (pLeaves.length > 1)
        logger.info(s"Point $pID : Found Duplicates (# = ${pLeaves.length}) in leaves = ${pLeaves.toString()}")
      else if (pLeaves.isEmpty)
        logger.info(s"Point $pID : Not found in leaves")
    }
  }

  private def validateAggrCounters(): Unit = {
    logger.info("\nAggregation (count) inconsistencies:\n")

    val rootNode = indexfile.retrieveNode(indexfile.getRootID)
    if(rootNode.getSCount != indexfile.getNumOfPoints)
      logger.info(s"Root [${rootNode.getNodeID}].SCount = ${rootNode.getSCount} isDiffThan total # points in dataset = ${indexfile.getNumOfPoints}\t\t ${rootNode.serialize}")

    for (nodeID <- 1 until indexfile.getNumOfNodes + 1) {
      val node = indexfile.retrieveNode(nodeID)
      val entriesCountSum = node.getEntries.map(entry => entry.getCount).sum

      if(node.getSCount != entriesCountSum)
        logger.info(s"Node [$nodeID].SCount = ${node.getSCount} isDiffThan sum of entries' count = $entriesCountSum \t " +
          s"isLeaf ${node.isLeaf} \t#entries = ${node.getNumberOfEntries} : \t" +
          s"${node.getEntries.map(e => if(node.isLeaf) e.asInstanceOf[Point].getPointID else e.asInstanceOf[Rectangle].getChildID)}" +
          s" \t\t ${node.serialize}")

      if(!node.isLeaf)
        node.zipWithIndex.foreach{case (mbr, i) =>
          val childNode = indexfile.retrieveNode(mbr.asInstanceOf[Rectangle].getChildID)
          if(mbr.getCount != childNode.getSCount)
            logger.info(s"MBR [$nodeID][$i].count = ${mbr.getCount} isDiffThan [${childNode.getNodeID}].SCount = ${childNode.getSCount}" +
              s"\t\tMBR = $mbr \t\t childNode = ${childNode.serialize}")
        }
      else if(node.getNumberOfEntries != node.getSCount)
        logger.info(s"Leaf node [${node.getNodeID}].SCount = ${node.getSCount} isDiffThan # point entries = ${node.getNumberOfEntries}")
    }
  }



}
