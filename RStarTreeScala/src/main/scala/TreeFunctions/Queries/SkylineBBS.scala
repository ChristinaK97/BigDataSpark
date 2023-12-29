package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.{LeafNode, TreeNode}
import Util.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SkylineBBS(indexFile: IndexFile, logger: Logger) {

  private val (rootID: Int, _) = indexFile.getRootIdAndTreeHeight
  private val sky : ListBuffer[(Point, Rectangle)] = ListBuffer[(Point, Rectangle)]()
  private val heap = mutable.PriorityQueue.empty[(GeometricObject, Double)](
                Ordering.by[(GeometricObject, Double), Double](_._2).reverse)

  logger.info("-"*100 + "\nCompute Skyline\n" + "-"*100)
  computeSkyline()



  private def computeSkyline(): Unit = {
    addToHeap(indexFile.retrieveNode(rootID))                                                                           
    while(heap.nonEmpty) {
      val (minEntry: GeometricObject, l1: Double) = heap.dequeue()

      minEntry match {
        case p: Point     => sky += ((p, p.dominanceArea))
        case r: Rectangle => addToHeap(indexFile.retrieveNode(r.getChildID))
      }
    }/*end while not empty*/
  }


  private def addToHeap(node: TreeNode): Unit = {
    node.zipWithIndex.foreach{case (geoObj, entryIndex) =>
      if(isDominant(entryIndex, geoObj, node))
        heap.enqueue((geoObj, geoObj.L1))
    }
  }



  private def isDominant(entryIndex: Int, geoObj: GeometricObject, node: TreeNode): Boolean = {
    !isDominatedBySky(geoObj) &&
    ( !node.isLeaf || node.asInstanceOf[LeafNode].isDominantInNode(entryIndex) )
  }


  private def isDominatedBySky(geoObj: GeometricObject): Boolean = {
    sky.foreach{case (_, skyPointDomArea) =>
      if(skyPointDomArea.contains(geoObj))
        return true
    }
    false
  }


// Public - retrieve results  -----------------------------------------------------------------
  def getSkylineWithDomAreas: ListBuffer[(Point, Rectangle)] = sky

  def getSkylinePoints: ListBuffer[Point] =
    sky.map { case (p, _) => p }




}
