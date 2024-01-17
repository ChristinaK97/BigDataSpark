package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{Point, Rectangle}
import TreeStructure.TreeNode



class MergePointCount(points: Iterable[Point], indexFile: IndexFile, decentralized: Boolean) {

  private val root = indexFile.retrieveNode(indexFile.getRootID)
  val partialSum: Any =
    if(decentralized) points.map(point => point.getPointID -> DecentralizedPointCount(root, point, 0)).toMap
    else points.foreach(p => CentralizedPointCount(root, p))


  private def CentralizedPointCount(node: TreeNode, point: Point): Unit = {
    node.foreach { nodeEntry =>

      if (!node.isLeaf && hasPartialDom(nodeEntry.asInstanceOf[Rectangle], point)) {
        val childID = nodeEntry.asInstanceOf[Rectangle].getChildID
        CentralizedPointCount(indexFile.retrieveNode(childID), point)

      } else if (point.dominates(nodeEntry))
        point.increaseDomScore(nodeEntry.getCount)
    }
  }


  private def DecentralizedPointCount(node: TreeNode, point: Point, sum: Int): Int = {
    node.foldLeft(sum) { (acc, nodeEntry) =>

      if (!node.isLeaf && hasPartialDom(nodeEntry.asInstanceOf[Rectangle], point)) {
        val childID = nodeEntry.asInstanceOf[Rectangle].getChildID
        DecentralizedPointCount(indexFile.retrieveNode(childID), point, acc)

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
