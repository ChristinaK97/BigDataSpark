package FileHandler

import Geometry.{Point, Rectangle}
import TreeStructure.{LeafNode, NonLeafNode, TreeNode}
import Util.Constants.N

import scala.collection.mutable.ListBuffer

object IndexFileReadWrite {

  def main(args: Array[String]): Unit = {
    N = 2
    val indexFile = new IndexFile(100, true)
    // test write
    test_write_non_leaf_node(indexFile)
    test_write_leaf_node(indexFile)
    test_update_non_leaf_node(indexFile)
    test_write_new_leaf_node(indexFile)

    // test read
    test_read_leaf_node(indexFile, 2)
    test_read_leaf_node(indexFile, 3)
    test_read_non_leaf_node(indexFile)
    indexFile.closeFile()
  }

  // write -------------------------------------------------------------------------------------------------------------
  def test_write_leaf_node(indexFile: IndexFile):Unit = {
    val leafP1 = new Point(Array(2.3, -5))
    val leafP2 = new Point(Array(-0.889, 0.125))
    val leaf = new LeafNode(2, ListBuffer(leafP1, leafP2))

    indexFile.writeNodeToFile(leaf)
  }

  def test_write_non_leaf_node(indexFile: IndexFile):Unit = {
    val rootR1 = new Rectangle(2, Array(2.3, -5), Array(0.8, -3.4))
    val rootR2 = new Rectangle(3, Array(0.3, 4), Array(-0.889, 0.125))
    val root = new NonLeafNode(1, ListBuffer(rootR1, rootR2))

    indexFile.writeNodeToFile(root)
  }

  def test_update_non_leaf_node(indexFile: IndexFile): Unit = {
    val rootR1 = new Rectangle(2, Array(2.3, -5), Array(0.8, -3.4))
    val rootR2 = new Rectangle(3, Array(0.3, 4), Array(-0.889, 0.125))
    val rootR3 = new Rectangle(4, Array(6.1, 3.2), Array(-0.8, 1.25))
    val root = new NonLeafNode(1, ListBuffer(rootR1, rootR2, rootR3))

    indexFile.writeNodeToFile(root)
  }

  def test_write_new_leaf_node(indexFile: IndexFile): Unit = {
    val leafP1 = new Point(Array(6.1, 3.2))
    val leafP2 = new Point(Array(-0.8, 0.5))
    val leaf = new LeafNode(3, ListBuffer(leafP1, leafP2))

    indexFile.writeNodeToFile(leaf)
  }
  // read --------------------------------------------------------------------------------------------------------------

  def test_read_leaf_node(indexFile: IndexFile, nodeID: Int): Unit = {
    println(s"\nTest read leaf node ${nodeID}")
    val leaf = indexFile.retrieveNode(nodeID).asInstanceOf[LeafNode]
    println(leaf.serialize)
    println(leaf.getPoint(0).serialize)
    println(leaf.getPoint(1).serialize)
  }

  def test_read_non_leaf_node(indexFile: IndexFile) : Unit = {
    println("\nTest read non leaf node 1")
    val nonLeaf = indexFile.retrieveNode(1).asInstanceOf[NonLeafNode]
    println(nonLeaf.serialize)
    nonLeaf.foreach(rectangle =>
      println(s"${rectangle.serialize} -> \t" +
        s"child= ${rectangle.asInstanceOf[Rectangle].getChildID} " +
        s"pm= ${rectangle.asInstanceOf[Rectangle].get_pm.serialize} " +
        s"pM= ${rectangle.asInstanceOf[Rectangle].get_pM.serialize} "))
  }

}
