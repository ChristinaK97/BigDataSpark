package TreeFunctions

import FileHandler.IndexFile
import Geometry.{Point, Rectangle}
import TreeFunctions.CreateTreeFunctions.CreateTree
import TreeFunctions.Queries.SkylineBBS
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

  def validateDataConsistency(): Unit = {

  }

}
