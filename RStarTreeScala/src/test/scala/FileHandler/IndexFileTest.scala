package FileHandler

import Geometry.{Point, Rectangle}
import TreeStructure.{LeafNode, NonLeafNode}
import Util.Constants.N
import org.scalatest.funsuite.AnyFunSuite

import java.lang.reflect.Method
import scala.collection.mutable.ListBuffer

class IndexFileTest extends AnyFunSuite  {

  test("Serialize leaf node") {
    N = 2
    val rootP1 = new Point(Array(2.3, -5))
    val rootP2 = new Point(Array(-0.889, 0.125))
    val root = new LeafNode(1, ListBuffer(rootP1, rootP2))
    val serializedRoot = "1|1|2.3,-5.0|-0.889,0.125"
    assert(serializedRoot == root.serialize)
  }

  test("Deserialize leaf node") {
    N = 2
    val rootP1 = new Point(Array(2.3, -5))
    val rootP2 = new Point(Array(-0.889, 0.125))
    val root = new LeafNode(1, ListBuffer(rootP1, rootP2))

    val indexFile = new IndexFile(100, true)
    val deserializeTreeNode: Method = classOf[IndexFile].getDeclaredMethod("deserializeTreeNode", classOf[String])
    deserializeTreeNode.setAccessible(true)
    val deserializedNode: LeafNode = deserializeTreeNode.invoke(indexFile, root.serialize).asInstanceOf[LeafNode]

    assert(root.serialize == deserializedNode.serialize)

    // P1
    assert("2.3,-5.0" == deserializedNode.getPoint(0).serialize)
    // P2
    assert("-0.889,0.125" == deserializedNode.getPoint(1).serialize)

    indexFile.closeFile()
  }

// --------------------------------------------------------------------------------------------------------------------
  test("Serialize Rectangle") {
    N = 2
    val rootR1 = new Rectangle(2, Array(2.3, -5), Array(0.8, -3.4))
    assert("2,2.3,-5.0,0.8,-3.4" == rootR1.serialize)
  }

  test("Serialize non leaf node") {
    N = 2
    val rootR1 = new Rectangle(2, Array(2.3, -5), Array(0.8, -3.4))
    val rootR2 = new Rectangle(3, Array(0.3, 4), Array(-0.889, 0.125))
    val root = new NonLeafNode(1, ListBuffer(rootR1, rootR2))
    val serializedRoot = "1|0|2,2.3,-5.0,0.8,-3.4|3,0.3,4.0,-0.889,0.125"
    assert(serializedRoot == root.serialize)
  }

  test("Deserialize non leaf node") {
    N = 2
    val rootR1 = new Rectangle(2, Array(2.3, -5), Array(0.8, -3.4))
    val rootR2 = new Rectangle(3, Array(0.3, 4), Array(-0.889, 0.125))
    val root = new NonLeafNode(1, ListBuffer(rootR1, rootR2))

    val indexFile = new IndexFile(100, true)
    val deserializeTreeNode: Method = classOf[IndexFile].getDeclaredMethod("deserializeTreeNode", classOf[String])
    deserializeTreeNode.setAccessible(true)
    val deserializedNode: NonLeafNode = deserializeTreeNode.invoke(indexFile, root.serialize).asInstanceOf[NonLeafNode]

    assert(root.serialize == deserializedNode.serialize)

    // R1
    assert("2.3,-5.0" == deserializedNode.getRectangle(0).get_pm.serialize)
    assert("0.8,-3.4" == deserializedNode.getRectangle(0).get_pM.serialize)
    // R2
    assert("0.3,4.0" == deserializedNode.getRectangle(1).get_pm.serialize)
    assert("-0.889,0.125" == deserializedNode.getRectangle(1).get_pM.serialize)

    indexFile.closeFile()
  }

// --------------------------------------------------------------------------------------------------------------------



}






