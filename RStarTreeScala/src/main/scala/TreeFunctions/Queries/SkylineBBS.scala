package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.{LeafNode, TreeNode}
import Util.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SkylineBBS(indexFile: IndexFile, logger: Logger) {

  private val rootID = indexFile.getRootID
  private val sky : ListBuffer[(Point, Rectangle)] = ListBuffer[(Point, Rectangle)]()
  private val heap = mutable.PriorityQueue.empty[(GeometricObject, Double)](
                Ordering.by[(GeometricObject, Double), Double](_._2).reverse)

  logger.info("-"*100 + "\nCompute Skyline\n" + "-"*100)
  computeSkyline()
  logger.info(toString)



  private def computeSkyline(): Unit = {
    addToHeap(indexFile.retrieveNode(rootID))
    while(heap.nonEmpty) {
      val (minEntry: GeometricObject, l1: Double) = heap.dequeue()                                                      ; logger.info(s"\nL1 = $l1 \t  ${minEntry.serialize}")

      minEntry match {
        case p: Point     => sky += ((p, p.dominanceArea))                                                              ; logger.info(s"Add to skyline \t < ${p.serialize} > \t # skyline = ${sky.length}")
        case r: Rectangle => addToHeap(indexFile.retrieveNode(r.getChildID))
      }
    }/*end while not empty*/
  }


  private def addToHeap(node: TreeNode): Unit = {
    node.zipWithIndex.foreach{case (geoObj, entryIndex) =>
      if(isDominant(entryIndex, geoObj, node)) {
        heap.enqueue((geoObj, geoObj.L1))                                                                               ; logger.info(s"\tAdd to heap \t < ${heap.head._2} : ${geoObj.serialize} > \t # heap = ${heap.length}")
      }
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



  override def toString: String = {
    val sb: StringBuilder = new StringBuilder()
    sb.append(s"\nSkyline Results :\n\t# points = ${sky.length}\n\t")
    sky.foreach{case (p, domArea) =>
      sb.append(p.serialize) //.append(s"\t\tDomArea = ${domArea.serialize}")
      sb.append("\n\t")
    }
    sb.toString()
  }
}
