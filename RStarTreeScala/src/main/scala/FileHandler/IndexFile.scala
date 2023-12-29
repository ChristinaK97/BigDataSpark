package FileHandler

import Geometry.{Point, Rectangle}
import TreeStructure.{LeafNode, NonLeafNode, TreeNode}
import Util.Constants.{N, UP_LIMIT}

import java.io.{File, RandomAccessFile}
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

class IndexFile(rTreeID: Long) {

  private val K: Int = UP_LIMIT / (16 * N + 4)
  private var BLOCK_CAPACITY: Int = 8 + K * (4 + 44 * N)

  private val INDEXFILE_PATH = s"indexfile_$rTreeID.txt"
  resetIndexfile()
  private val indexfile = new RandomAccessFile(INDEXFILE_PATH, "rw")
  private val metadata: Metadata = new Metadata()

  private var IOs: Int = 0

  // for testing
  def this(CAPACITY: Int, testing: Boolean) = {
    this(-1)
    BLOCK_CAPACITY = CAPACITY
  }



// Tree Metadata  ------------------------------------------------------------------------------------------------------

  def getRootIdAndTreeHeight: (Int, Int) =
    (metadata.getRootID, metadata.getTreeHeight)

  def getTreeMetadata: Metadata = metadata

  def updateMetadata(rootID: Int, treeHeight: Int) : Unit = {
    metadata.setRootID(rootID)
    metadata.setTreeHeight(treeHeight)
  }

  def getIOs: Int = IOs

// Write & Read  -------------------------------------------------------------------------------------------------------

  /**
   * nodeInfo :
   * Leaf Node :
   *    header: nodeInfo[0] : "nodeID|1|"
   *    data:   nodeInfo[1] : "Point1     |...|PointK"
   *                        : "x1,y1,..,n1|...|xK,yK,..,nK
   *
   * Non Leaf Node :
   *    header: nodeInfo[0]= "nodeID|0|"
   *    data:   nodeInfo[1]: "MBR1|...|MBRK"
   *                       : "childNodeID,  pm_x1,pm_y1,..,pm_n1 , pM_x1,pM_y1,..,pM_n1|...
   *                          -----------   --------------------   --------------------
   *                            childPTR    pm coordinates[1..N]   pM coordinates[N+1..2N+1]
   *
   * nodeID|0 ή 1|Data1|Data2|...|DataN
   *
   */

  /** Αποθηκεύει στο indexfile το TreeNode node.
   * Αν υπήρχε ήδη στο αρχείο αυτό το node, τότε ενημερώνεται.
   * Αν είναι νέο node τότε γράφεται στην τελευταία γραμμή του αρχείου.
   */
  def writeNodeToFile(node: TreeNode): Unit = {
    IOs += 1
    var (begin: Long, _: Int) = metadata.getBlockPos(node.getNodeID)
    val data = turnToBinary(node.serialize)
    val newSize = data.length
    assert(newSize < BLOCK_CAPACITY, s"Node ${node.getNodeID}: Block size ${newSize} exceeded block capacity. " +
      s"\n\tMAX # entries in non-leaf node = $K\t MAX block capacity = $BLOCK_CAPACITY")

    if(begin == -1) {
      begin = nextPos
      val emptySlot = Array.fill[Byte](BLOCK_CAPACITY - newSize)(' '.toByte )
      emptySlot.update(emptySlot.length-1, '\n'.toByte)
      writeToPos(emptySlot, begin + newSize)
    }
    metadata.addBlock(node.getNodeID, begin, newSize)
    writeToPos(data, begin)
  }


  /** Καλείται για να διαβάσει το node με id== nodeId από το αρχείο
   * indexfile.
   *
   * @param nodeID : Το id του node που θα διαβαστεί
   * @return : Ένα αντικείμενο TreeNode του node που θα διαβάσει και
   *         το επιστρέφει. Αν το node είναι φύλλο, είναι instanceof LeafNode.
   *         Αν το node είναι εσωτερικός κόμβος ή η ρίζα, είναι instanceof NonLeafNode.
   */
  def retrieveNode(nodeID: Int): TreeNode = {
    IOs += 1
    val (begin: Long, size: Int) = metadata.getBlockPos(nodeID)
    assert(begin != -1, s"Node with id ${nodeID} not found in index file")
    deserializeTreeNode(readData(begin, size))
  }



// Random Access File  -------------------------------------------------------------------------------------------------


  private def resetIndexfile() : Unit = {
    val file = new File(INDEXFILE_PATH)
    if (!file.exists() || file.delete())
      file.createNewFile();
  }

  def closeFile(): Unit =
    indexfile.close()


  /** Επιστρέφει το δείκτη του τέλους του αρχείου */
  private def nextPos: Long =
    indexfile.length

  /** Επιστρέφει πίνακα με τη δυαδική μορφή της συμβολοσειράς data. */
  private def turnToBinary(data: String): Array[Byte] =
    data.getBytes(StandardCharsets.UTF_8)



  /** Γράφει τα δεδομένα με αρχή το pos στο αρχείο */
  private def writeToPos(data: Array[Byte], pos: Long): Unit = {
    indexfile.seek(pos)
    indexfile.write(data)
  }

  /** Διάβασε τα δεδομένα από το αρχείο από τη θέση begin μέχρι
   * τη θέση begin+size, τα δεδομένα ενός slot δηλαδή.
   */
  private def readData(begin: Long, size: Int): String =
    new String(readBytes(begin, size), StandardCharsets.UTF_8)


  /** Διάβασε τα δεδομένα από το αρχείο από τη θέση begin μέχρι
   * τη θέση begin+size, τα δεδομένα ενός slot δηλαδή. */
  private def readBytes(begin: Long, size: Int): Array[Byte] = {
    val data = new Array[Byte](size)
    indexfile.seek(begin)
    indexfile.read(data)
    data
  }


// Deserializers -------------------------------------------------------------------------------------------------------
  private def deserializeTreeNode(serializedNode: String): TreeNode = {
    val nodeData = serializedNode.split("\\|")

    deserializeTreeNode(
      nodeData(0).toInt,       //nodeID
      nodeData(1).equals("1"), //isLeaf
      nodeData.drop(2).map(serializedEntry => serializedEntry.split(",")),
    )

  }

  private def deserializeTreeNode(nodeID: Int, isLeaf: Boolean, serializedEntries: Array[Array[String]]): TreeNode = {
    if(isLeaf)
      new LeafNode(nodeID, deserializeLeafNodeEntries(serializedEntries))
    else
      new NonLeafNode(nodeID, deserializeNonLeafNodeEntries(serializedEntries))
  }

  private def deserializeLeafNodeEntries(serializedEntries: Array[Array[String]]): ListBuffer[Point] = {
    serializedEntries.map(serializedPoint =>
        new Point(serializedPoint.map(_.toDouble))
    ).to(ListBuffer)
  }

  private def deserializeNonLeafNodeEntries(serializedEntries: Array[Array[String]]): ListBuffer[Rectangle] = {
    serializedEntries.map(serializedRectangle =>
      new Rectangle(
        serializedRectangle(0).toInt,
        serializedRectangle.slice(1,N+1).map(_.toDouble),
        serializedRectangle.drop(N+1).map(_.toDouble)
      )
    ).to(ListBuffer)
  }

}
