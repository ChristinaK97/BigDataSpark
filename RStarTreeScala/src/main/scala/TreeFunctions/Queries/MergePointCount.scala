package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{Point, Rectangle}
import TreeStructure.TreeNode


class MergePointCount(points: Array[Point], indexFile: IndexFile) {

  private val root = indexFile.retrieveNode(indexFile.getRootID)
  points.foreach(p => PointCount(root, p))

  private def PointCount(node: TreeNode, point: Point): Unit = {

    node.foreach { nodeEntry =>

      if (!node.isLeaf && hasPartialDom(nodeEntry.asInstanceOf[Rectangle], point)) {
        val childID = nodeEntry.asInstanceOf[Rectangle].getChildID
        PointCount(indexFile.retrieveNode(childID), point)

      } else if (point.dominates(nodeEntry))
        point.increaseDomScore(nodeEntry.getCount)

    } //end foreach nodeEntry
  }


  private def hasPartialDom(r: Rectangle, point: Point): Boolean = {
    !point.dominates(r.get_pm) && point.dominates(r.get_pM)
  }

}
