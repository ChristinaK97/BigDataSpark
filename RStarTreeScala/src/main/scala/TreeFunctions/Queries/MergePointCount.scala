package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{Point, Rectangle}
import TreeStructure.TreeNode



class MergePointCount(points: Iterable[Point], indexFile: IndexFile) {

  private val root = indexFile.retrieveNode(indexFile.getRootID)
  points.foreach(p => PointCount(root, p))
  //val partialSum: Map[Int, Int] = points.map(point => point.getPointID -> PointCount(root, point, 0)).toMap

  private def PointCount(node: TreeNode, point: Point /*, sum: Int*/): Unit /*Int*/ = {
    //var sumVar = sum
    node.foreach { nodeEntry =>

      if (!node.isLeaf && hasPartialDom(nodeEntry.asInstanceOf[Rectangle], point)) {
        val childID = nodeEntry.asInstanceOf[Rectangle].getChildID
        PointCount(indexFile.retrieveNode(childID), point /*, sumVar*/)

      } else if (point.dominates(nodeEntry))
        point.increaseDomScore(nodeEntry.getCount)
        //sumVar += nodeEntry.getCount

    }
    //sumVar
  }

  private def hasPartialDom(r: Rectangle, point: Point): Boolean = {
    !point.dominates(r.get_pm) && point.dominates(r.get_pM)
  }

}
