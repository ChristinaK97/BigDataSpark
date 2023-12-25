package FileHandler

import scala.collection.mutable

class Metadata {

  private val nodeIndexer: mutable.HashMap[Int, (Long, Int)] = new mutable.HashMap[Int, (Long, Int)]()


  def addBlock(nodeID: Int, begin: Long, size: Int): Unit =
    nodeIndexer.put(nodeID, (begin, size))

  def getBlockPos(nodeID: Int): (Long, Int) =
    nodeIndexer.getOrElse(nodeID, (-1,-1))

}
