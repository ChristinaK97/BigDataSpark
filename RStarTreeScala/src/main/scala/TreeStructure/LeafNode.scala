package TreeStructure

import Geometry.{GeometricObject, Point}

import scala.collection.mutable.ListBuffer

class LeafNode(nodeId: Int) extends TreeNode(nodeId, null) {

  def this(nodeId: Int, entries: ListBuffer[Point]) = {
    this(nodeId)
    setEntries(entries.asInstanceOf[ListBuffer[GeometricObject]])
  }

  override def isLeaf: Boolean = true

  /** @param index : Η θέση του entriesOnNode
   * @return Η εγγραφή (Point) στη θέση index του entriesOnNode */
  def getPoint(index: Int): Point =
    super.getEntry(index).asInstanceOf[Point]


/* ---------------------- Ταξινόμηση των εγγραφών του κόμβου-------------------------------*/
  /** @return μια λίστα με ταξινομιμένες (αύξουσα) τις εγγραφές
   * του κόμβου σύμφωνα με τον άξονα axis. Ο κόμβος είναι φύλλο
   * άρα οι εγγραφές του είναι Points. Θα ταξινομηθούν με βάση
   * την τιμή των points στη διάσταση axis: point[axis].
   * 1η διάσταση: axis = 0 (x)
   */
  def sortEntriesBy(axis: Int): ListBuffer[Point] =
    getEntries.asInstanceOf[ListBuffer[Point]].sortBy(
      _.getCoordinate(axis)
    )

}
