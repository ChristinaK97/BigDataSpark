package FileHandler

import Geometry.{Point, Rectangle}
import TreeStructure.{LeafNode, NonLeafNode, TreeNode}
import Util.Constants.{N, UP_LIMIT}

import java.io.{File, RandomAccessFile}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.ListBuffer

class IndexFile(resetTree: Boolean, rTreeID: String) {

  private val K: Int = UP_LIMIT / (16 * N + 4)
  private var BLOCK_CAPACITY: Int = 8 + 7 + K * (7 + 4 + 44 * N)

  val PARTITION_DIR: Path = Paths.get(s"RTrees/RTreePartition_$rTreeID")
  private val INDEXFILE_PATH = Paths.get(s"$PARTITION_DIR/indexfile.txt").toString
  private val METADATA_PATH  = Paths.get(s"$PARTITION_DIR/metadata.txt").toString

  private var metadata: Metadata = _
  private val treeWasReset: Boolean = prepareTreeFiles()
  private val indexfile = new RandomAccessFile(INDEXFILE_PATH, "rw")

  private var IOs: Int = 0


// ---------------------------------------------------------------------------------------------------------------------
  private def prepareTreeFiles(): Boolean = {
    var resetTreeVar = resetTree
    val indexfileFile = new File(INDEXFILE_PATH)
    val metadataFile  = new File(METADATA_PATH)
    if(resetTree || !indexfileFile.exists() || !metadataFile.exists()) {
      resetTreeVar = true
      resetFiles(indexfileFile, metadataFile)
    }
    if(resetTreeVar)
      metadata = new Metadata()
    else {
      try {
        metadata = MetadataLoader.loadFromFile(METADATA_PATH)
      } catch {
        case e: Exception =>
          println(e.getMessage)
          resetTreeVar = true
          resetFiles(indexfileFile, metadataFile)
          metadata = new Metadata()
      }
    }
    resetTreeVar
  }
  private def resetFiles(indexfileFile: File, metadataFile: File): Unit = {
    if(Files.exists(PARTITION_DIR)) {
      indexfileFile.delete()
      metadataFile.delete()
    }else
      Files.createDirectories(PARTITION_DIR)

    indexfileFile.createNewFile()
    metadataFile.createNewFile()
  }

// Tree Metadata  ------------------------------------------------------------------------------------------------------

  def updateMetadata(rootID: Int, treeHeight: Int, numOfNodes: Int) : Unit = {
    metadata.setRootID(rootID)
    metadata.setTreeHeight(treeHeight)
    metadata.setNumOfNodes(numOfNodes)
  }

  def getIOs: Int = IOs
  def getTreeWasReset: Boolean = treeWasReset
  def getTreeMetadata: Metadata = metadata
  def getRootID: Int = metadata.getRootID
  def getTreeHeight: Int = metadata.getTreeHeight
  def getNumOfNodes: Int = metadata.getNumOfNodes
  def getNumOfPoints: Int = metadata.getNumOfPoints
  def setNumOfPoints(numOfPoints: Int): Unit = metadata.setNumOfPoints(numOfPoints)

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

  def closeFile(): Unit = {
    indexfile.close()
    metadata.saveToFile(METADATA_PATH)
  }


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
      nodeData(2).toInt,       //SCount
      nodeData.drop(3).map(serializedEntry => serializedEntry.split(",")),
    )

  }

  private def deserializeTreeNode(nodeID: Int, isLeaf: Boolean, SCount: Int, serializedEntries: Array[Array[String]]): TreeNode = {
    if(isLeaf)
      new LeafNode(nodeID, SCount, deserializeLeafNodeEntries(serializedEntries))
    else
      new NonLeafNode(nodeID, SCount, deserializeNonLeafNodeEntries(serializedEntries))
  }

  private def deserializeLeafNodeEntries(serializedEntries: Array[Array[String]]): ListBuffer[Point] = {
    serializedEntries.map(serializedPoint =>
        new Point(
          serializedPoint.head.toInt,
          serializedPoint.drop(1).map(_.toDouble))
    ).to(ListBuffer)
  }

  private def deserializeNonLeafNodeEntries(serializedEntries: Array[Array[String]]): ListBuffer[Rectangle] = {
    serializedEntries.map(serializedRectangle =>
      new Rectangle(
        serializedRectangle(0).toInt,
        serializedRectangle(1).toInt,
        serializedRectangle.slice(2,N+2).map(_.toDouble),
        serializedRectangle.drop(N+2).map(_.toDouble)
      )
    ).to(ListBuffer)
  }

// ---------------------------------------------------------------------------------------------------------------------

  // for testing
  def this(CAPACITY: Int, testing: Boolean) = {
    this(true, "-1")
    BLOCK_CAPACITY = CAPACITY
  }
}
