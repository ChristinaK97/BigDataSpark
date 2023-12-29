package FileHandler

import Util.Constants.N

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable

class Metadata extends Serializable {

  private val nodeIndexer: mutable.HashMap[Int, (Long, Int)] = new mutable.HashMap[Int, (Long, Int)]()
  private var rootID = 1
  private var treeHeight = 1
  private var numOfNodes = 1
  private var numOfPoints = 0

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

  def getNumOfPoints: Int = numOfPoints
  def setNumOfPoints(nPoints: Int): Unit = {numOfPoints = nPoints}


  // Method to save Metadata object to a file
  def saveToFile(filePath: String): Unit = {
    val fileOutputStream = new FileOutputStream(filePath)
    val objectOutputStream = new ObjectOutputStream(fileOutputStream)

    objectOutputStream.writeObject(this)

    objectOutputStream.close()
    fileOutputStream.close()
  }

}

object MetadataLoader {
  // Method to load Metadata object from a file
  def loadFromFile(filePath: String): Metadata = {
    val fileInputStream = new FileInputStream(filePath)
    val objectInputStream = new ObjectInputStream(fileInputStream)

    val metadata = objectInputStream.readObject().asInstanceOf[Metadata]

    objectInputStream.close()
    fileInputStream.close()

    metadata
  }
}
