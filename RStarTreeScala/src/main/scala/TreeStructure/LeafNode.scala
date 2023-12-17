package TreeStructure

import Geometry.Point

class LeafNode extends TreeNode {

  override def isLeaf: Boolean = true

  /** @param index : Η θέση του entriesOnNode
   * @return Η εγγραφή (Point) στη θέση index του entriesOnNode */
  def getPoint(index: Int): Point =
    super.getEntry(index).asInstanceOf[Point]
}
