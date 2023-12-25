package TreeStructure

import Geometry.GeometricObject
import Util.Constants.UP_LIMIT

import scala.collection.mutable.ListBuffer

abstract class TreeNode(nodeId: Int, entries: ListBuffer[GeometricObject]) extends Iterable[GeometricObject] {

  private val nodeID: Int = nodeId
  private var NBytesInNode: Int = _
  private var entriesOnNode: ListBuffer[GeometricObject] = _
  setEntries(entries)


  /** Iterator for entriesOnNode */
  override def iterator: Iterator[GeometricObject] =
    entriesOnNode.iterator

  def getNodeID: Int =
    nodeID

  def getNumberOfEntries: Int =
    entriesOnNode.length


  def setEntries(entries: ListBuffer[GeometricObject]): Unit = {
    if (entries == null) {
      entriesOnNode = ListBuffer[GeometricObject]()
      NBytesInNode = 0
    } else {
      entriesOnNode = entries
      NBytesInNode = getNumberOfEntries * entries.head.getMemorySize
    }
  }


  def getEntries: ListBuffer[GeometricObject] =
    entriesOnNode

  def getEntry(index: Int): GeometricObject =
    entriesOnNode(index)


  def addEntry(entry: GeometricObject): Unit = {
    entriesOnNode += entry
    NBytesInNode += entry.getMemorySize
  }

  def deleteEntry(splitIndex: Int): Unit =
    entriesOnNode.remove(splitIndex)

  def deleteEntry(node: GeometricObject): Unit =
    entriesOnNode -= node

  def isFull: Boolean =
    NBytesInNode >= UP_LIMIT

  def isLeaf: Boolean

 def serialize: String = {
   val sb: StringBuilder = new StringBuilder()

   sb.append(nodeID).append("|")
     .append(if(isLeaf) "1" else "0").append("|")

   this.zipWithIndex.foreach{ case (entry, index) =>
     sb.append(entry.serialize)
     if(index < getNumberOfEntries-1)
       sb.append("|")
   }
   sb.toString()
 }


}
