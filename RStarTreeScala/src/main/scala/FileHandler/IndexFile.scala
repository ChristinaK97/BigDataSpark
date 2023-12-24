package FileHandler

import Geometry.{Point, Rectangle}
import TreeStructure.{LeafNode, NonLeafNode, TreeNode}

import scala.collection.mutable.ListBuffer

class IndexFile {
  private var IOs: Int = 0
  // TODO: read from main
  private val N = 5

  def updateMetadata(rootID: Int, treeHeight: Int) : Unit = {

  }

  def writeNodeToFile(node: TreeNode): Unit = {
    IOs += 1
  }

  def retrieveNode(nodeID: Int): TreeNode = {
    IOs += 1
    null
  }

  private def deserializeTreeNode(serializedNode: String): TreeNode = {
    val nodeData: Array[String] = serializedNode.split("\\|")
    val NBitsInNode: Int = nodeData(2).toInt

    deserializeTreeNode(
      nodeData(0).toInt,       //nodeID
      nodeData(1).equals("1"), //isLeaf
      nodeData.drop(3).map(serializedEntry => serializedEntry.split(",")),
    )

  }

  private def deserializeTreeNode(nodeID: Int, isLeaf: Boolean, serializedEntries: Array[Array[String]]): TreeNode = {
    if(isLeaf)
      new LeafNode(nodeID, deserializeLeafNodeEntries(serializedEntries))
    else
      new NonLeafNode(nodeID, deserializeNonLeafNodeEntries(serializedEntries))
  }

  private def deserializeLeafNodeEntries(serializedEntries: Array[Array[String]]): ListBuffer[Point] = {
    serializedEntries.map(serializedPoint =>
        new Point(serializedPoint.map(_.toDouble))
    ).to(ListBuffer)
  }

  private def deserializeNonLeafNodeEntries(serializedEntries: Array[Array[String]]): ListBuffer[Rectangle] = {
    serializedEntries.map(serializedRectangle =>
      new Rectangle(
        serializedRectangle(0).toInt,
        serializedRectangle.slice(1,N).map(_.toDouble),
        serializedRectangle.drop(N+1).map(_.toDouble)
      )
    ).to(ListBuffer)
  }

}
