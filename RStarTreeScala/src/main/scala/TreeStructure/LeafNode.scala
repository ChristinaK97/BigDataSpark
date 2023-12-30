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

/* ---------------------------------- Dominance Queries ------------------------------------*/

  /** Αν το geoObj isa Point από ένα leaf node, δεν κυριαρχείται
   * από κανένα άλλο σημείο μέσα στο node, αν
   *   - εξίσου καλό σε όλες τις διαστάσεις και
   *   - καλύτερο σε τουλάχιστον μία.
   * Πρέπει να ικανοποιεί αυτή τη σχέση με κάθε άλλο σημείο που ανήκει στο node.
   *
   * @param pIndex: Η θέση του σημείου που εξετάζεται μέσα στο entriesInNode
   * @return true αν το σημείο δεν κυριαρχείται από κανένα άλλο σημείου του leaf node.
   */
  def isDominantInNode(pIndex: Int): Boolean = {
    val p: Point = getEntry(pIndex).asInstanceOf[Point]
    this.zipWithIndex.foreach{case (q, qIndex) =>
      if(pIndex != qIndex && q.asInstanceOf[Point].dominates(p))
        return false
    }
    true
  }

}
