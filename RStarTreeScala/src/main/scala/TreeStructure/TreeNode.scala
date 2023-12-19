package TreeStructure

import Geometry.GeometricObject
import Util.Constants.UP_LIMIT

import scala.collection.mutable.ListBuffer

abstract class TreeNode(nodeId: Int) extends Iterable[GeometricObject] {

  private val nodeID: Int = nodeId
  private var NBitsInNode: Int = 0
  private var entriesOnNode: ListBuffer[GeometricObject] = ListBuffer[GeometricObject]() // empty mutable list


  /** Iterator for entriesOnNode */
  override def iterator: Iterator[GeometricObject] =
    entriesOnNode.iterator

  def getNodeID: Int =
    nodeID

  def getNumberOfEntries: Int =
    entriesOnNode.length

  def getEntries: ListBuffer[GeometricObject] =
    entriesOnNode

  def getEntry(index: Int): GeometricObject =
    entriesOnNode(index)

  def setEntries(entries: ListBuffer[GeometricObject]): Unit =
    this.entriesOnNode = entries

  def addEntry(node: GeometricObject): Unit = {
    entriesOnNode += node
    NBitsInNode += node.getMemorySize
  }

  def deleteEntry(splitIndex: Int): Unit =
    entriesOnNode.remove(splitIndex)

  def deleteEntry(node: GeometricObject): Unit =
    entriesOnNode -= node

  def isFull: Boolean =
    NBitsInNode >= UP_LIMIT

  def isLeaf: Boolean

  //def sortEntriesBy[T <: GeometricObject](axis:Int): ListBuffer[T]


}
