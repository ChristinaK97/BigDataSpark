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


  def createTree(rTreeID: Long) : Unit = {
    indexfile = new IndexFile(rTreeID)
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

  private def validateDataConsistency(): Unit = {
    val counter: Array[ListBuffer[Int]] = Array.fill[ListBuffer[Int]](indexfile.getNumOfPoints)(ListBuffer[Int]())
    for(nodeID <- 1 until indexfile.getNumOfNodes+1) {
      val node: TreeNode = indexfile.retrieveNode(nodeID)
      if(node.isLeaf)
        node.foreach{case p: Point =>
          counter(p.getPointID) += nodeID
        }
    }
    //logger.info("-" * 100 + "\nPoints in leaves :\n")
    //for (pID <- counter.indices) logger.info(s"Point $pID in # leaves = ${counter(pID).length} : ${counter(pID)}")

    logger.info("-" * 100 + "\nInconsistencies:\n")
    for(pID <- counter.indices) {
      val pLeaves: ListBuffer[Int] = counter(pID)
      if(pLeaves.length > 1)
        logger.info(s"Point $pID : Found Duplicates (# = ${pLeaves.length}) in leaves = ${pLeaves.toString()}")
      else if(pLeaves.isEmpty)
        logger.info(s"Point $pID : Not found in leaves")
    }
  }


}
