package FileHandler

import scala.collection.mutable

class Metadata {

  private val nodeIndexer: mutable.HashMap[Int, (Long, Int)] = new mutable.HashMap[Int, (Long, Int)]()
  private var rootID = 1
  private var treeHeight = 1
  private var numOfNodes = 1

  def addBlock(nodeID: Int, begin: Long, size: Int): Unit =
    nodeIndexer.put(nodeID, (begin, size))

  def getBlockPos(nodeID: Int): (Long, Int) =
    nodeIndexer.getOrElse(nodeID, (-1,-1))


  def getRootID: Int = rootID
  def setRootID(newRootID: Int): Unit = {rootID = newRootID}

  def getTreeHeight: Int = treeHeight
  def setTreeHeight(newHeight: Int): Unit = {treeHeight = newHeight}

  def getNumOfNodes: Int = numOfNodes
  def setNumOfNodes(newNumOfNodes: Int): Unit = {numOfNodes = newNumOfNodes}

}
