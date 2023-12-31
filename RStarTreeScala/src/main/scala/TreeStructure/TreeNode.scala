package TreeStructure

import Geometry.{GeometricObject, Point, Rectangle}
import Util.Constants.UP_LIMIT

import scala.collection.mutable.ListBuffer

abstract class TreeNode(nodeId: Int, entries: ListBuffer[GeometricObject]) extends Iterable[GeometricObject] {

  private val nodeID: Int = nodeId
  private var NBytesInNode: Int = _
  private var SCount: Int = 0
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

  def updateEntry(index: Int, updatedEntry: GeometricObject): Unit  =
    entriesOnNode(index) = updatedEntry


  def getEntries: ListBuffer[GeometricObject] =
    entriesOnNode

  def getEntry(index: Int): GeometricObject =
    entriesOnNode(index)


  def addEntry(entry: GeometricObject): Unit = {
    assert((isLeaf && entry.isInstanceOf[Point]) || (!isLeaf && entry.isInstanceOf[Rectangle]))
    entriesOnNode += entry
    NBytesInNode += entry.getMemorySize
    increaseSCount(entry)
  }

  def deleteEntry(splitIndex: Int): Unit =
    entriesOnNode.remove(splitIndex)


  def getNBytes: Int = NBytesInNode

  def isFull: Boolean =
    NBytesInNode >= UP_LIMIT

  def isLeaf: Boolean


 def serialize: String = {
   val sb: StringBuilder = new StringBuilder()

   sb.append(nodeID).append("|")
     .append(if(isLeaf) "1" else "0").append("|")
     .append(SCount).append("|")

   this.zipWithIndex.foreach{ case (entry, index) =>
     sb.append(entry.serialize)
     if(index < getNumberOfEntries-1)
       sb.append("|")
   }
   sb.toString()
 }

  override def toString: String =
    serialize



  /* ----------------------- Node Count Aggregation  ------------------------------*/
  def getSCount: Int = SCount
  def setSCount(newCount: Int): Unit = SCount = newCount
  def increaseSCount(value: Int): Unit = SCount += value
  def increaseSCount(geoObj: GeometricObject): Unit = increaseSCount(geoObj.getCount)
  def decreaseSCount(value: Int): Unit = SCount -= value
  def decreaseSCount(geoObj: GeometricObject): Unit = decreaseSCount(geoObj.getCount)
  def calculateSCount(): Unit = {
    setSCount(map(entry => entry.getCount).sum)
  }

}
