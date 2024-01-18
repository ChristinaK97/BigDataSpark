package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{Point, Rectangle}
import TreeStructure.TreeNode



class MergePointCount(indexFile: IndexFile) {

  private val thisPartitionID = indexFile.partitionID
  private val root = indexFile.retrieveNode(indexFile.getRootID)
  var partialSum: Any = _

  /** for centralized merge */
  def this (points: Iterable[Point], indexFile: IndexFile) = {
    this(indexFile)
    points.foreach(p => CentralizedPointCount(root, p))
  }

  /** for distributed merge with broadcasting all topK lists */
  def this(points: Array[(String, Point)], indexFile: IndexFile) = {
    this(indexFile)
    partialSum = points.map { case (pointPartitionID, point) =>
      point.getPointID ->
        (if (pointPartitionID.equals(thisPartitionID)) 0
        else DistributedPointCount(root, point, 0))
    }.toMap
  }


  private def CentralizedPointCount(node: TreeNode, point: Point): Unit = {
    node.foreach { nodeEntry =>

      if (!node.isLeaf && hasPartialDom(nodeEntry.asInstanceOf[Rectangle], point)) {
        val childID = nodeEntry.asInstanceOf[Rectangle].getChildID
        CentralizedPointCount(indexFile.retrieveNode(childID), point)

      } else if (point.dominates(nodeEntry))
        point.increaseDomScore(nodeEntry.getCount)
    }
  }


  private def DistributedPointCount(node: TreeNode, point: Point, sum: Int): Int = {
    node.foldLeft(sum) { (acc, nodeEntry) =>

      if (!node.isLeaf && hasPartialDom(nodeEntry.asInstanceOf[Rectangle], point)) {
        val childID = nodeEntry.asInstanceOf[Rectangle].getChildID
        DistributedPointCount(indexFile.retrieveNode(childID), point, acc)

      } else if (point.dominates(nodeEntry))
        acc + nodeEntry.getCount
      else
        acc
    }
  }


  private def hasPartialDom(r: Rectangle, point: Point): Boolean = {
    !point.dominates(r.get_pm) && point.dominates(r.get_pM)
  }

}
