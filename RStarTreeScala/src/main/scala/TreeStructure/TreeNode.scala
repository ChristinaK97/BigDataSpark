package TreeStructure

import Geometry.GeometricObject
import Util.Constants.UP_LIMIT

import scala.collection.mutable.ListBuffer

abstract class TreeNode extends Iterable[GeometricObject] {

  private var nodeID: Int = _
  private var entriesOnNode: ListBuffer[GeometricObject] = ListBuffer() // empty mutable list

  private var NBitsInNode: Int = 0

  def this(nodeID: Int) {
    this()
    this.nodeID = nodeID
  }

  /** Iterator for entriesOnNode */
  override def iterator: Iterator[GeometricObject] = entriesOnNode.iterator

  def getNodeID: Int = nodeID

  def getNumberOfEntries: Int = entriesOnNode.length

  def setEntries(entries: ListBuffer[GeometricObject]): Unit =
    this.entriesOnNode = entries

  def addEntry(node: GeometricObject): Unit = {
    entriesOnNode += node
    NBitsInNode += node.getMemorySize
  }

  def getEntry(index: Int): GeometricObject =
    entriesOnNode(index)

  def deleteEntry(splitIndex: Int): Unit =
    entriesOnNode.remove(splitIndex)

  def deleteEntry(node: GeometricObject): Unit =
    entriesOnNode -= node

  def isFull: Boolean =
    NBitsInNode >= UP_LIMIT

  def isLeaf: Boolean


}
