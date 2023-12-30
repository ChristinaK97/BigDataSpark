package TreeFunctions.CreateTreeFunctions

import FileHandler.IndexFile
import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.{LeafNode, NonLeafNode, TreeNode}
import Util.Logger

import scala.collection.mutable

class CreateTree(indexfile: IndexFile, logger : Logger) {

  /* κόμβος ρίζα του δέντρου, όταν είναι ο μοναδικός κόμβος του δέντρου είναι φύλλο
  */ private var root: TreeNode = new LeafNode(1)

  /* ύψος του δέντρου
  */ private var treeHeight: Int = 1

  /*το id που θα λάβει ο επόμενος κόμβος που θα δημιουργηθεί
  */ private var nextNodeID: Int = 2
  // ----------------------------------------------------------------------------------------

  /*Το μονοπάτι node από την ρίζα μέχρι το φύλλο που θα ακολουθηθεί
	* για την εισαγωγή μιας εγγραφής στο δέντρο
	* TreeNode: ο επιλεγμένος κόμβος
	* int: index στο entriesOnNode
	*   Για επίπεδα πάνω από το επίπεδο εισαγωγής: Το index του επιλεγμένου MBR στο entriesOnNode του node
	*   Για το επίπεδο εισαγωγής: Το index στο entriesOnNode όπου θα μπει το νέο entry (entriesOnNode.length)
  */ private var path: mutable.Stack[(TreeNode, Int)] = _

  /* GeometricObject: Εγγραφή που θα εισαχθεί στο δέντρο
   * Int: Το επίπεδο όπου πρέπει να εισαχθεί
   *    πχ αν geoObj isa Point : επίπεδο εισαγωγής -> φύλλα level==treeHeight
   *       αν geoObj isa Rectangle for ReInsert : επίπεδο εισαγωγής, το επίπεδο από όπου αφαιρέθηκε
  */ private val toInclude: mutable.Stack[(GeometricObject, Int)] = mutable.Stack[(GeometricObject, Int)]()

  /* Ολες οι εγγραφές ενός μπλοκ του datafile
    * που πρέπει να εισαχθούν στο δέντρο.
  */ private val toInsert: mutable.Stack[Point] = mutable.Stack[Point]()

  /* Εγραφές που βγήκαν από τον κόμβο και θα πρέπει να επαεισαχθούν
	 * Data: η εγγραφή
	 * int: level το επίπεδο του δέντρου από όπου βγήκε η εγγραφή
	*/ private var toReInsert: mutable.Stack[(GeometricObject,Int)] = mutable.Stack[(GeometricObject,Int)]()
  // ----------------------------------------------------------------------------------------

  /* Integer: level του δέντρου
	 * Boolean: αν εκτελέστηκε επανεισαγωγή σε αυτό το level του δέντρου
	*/ private val reinsertOnLevel: mutable.HashMap[Int, Boolean] = mutable.HashMap[Int, Boolean]()

  /* Για το ReInsert. Δημιουργήθηκε νέα ρίζα.
	*/ private var NewRootMade: Boolean = false

  /* Αν συνέβει ReInsert από την OverflowTreatment
	*/ private var ReInsert: Boolean = false

  /* Αν συνέβει Split από την OverflowTreatment
	*/ private var Split: Boolean = false

  private var counter: Int = 0


// ----------------------------------------------------------------------------------------

  /** 1. Καλείται για να δημιουργήσει ένα νέο δέντρο από τις
   * εγγραφές που είναι αποθηκευμένες στο datafile.
   */
  def this(indexFile: IndexFile, pointsPartition: Iterator[Point], logger : Logger) = {
    this(indexFile, logger)
    pointsPartition.foreach(point => {
      logger.info(s"\n>> Point $counter : ${point.serialize}")
      toInsert += point
      insertFrom_toReInsert_toInsert()
    })
    indexFile.writeNodeToFile(root)                                                                                     ; logger.info(s"# IOs = ${indexFile.getIOs}\t tree height = $treeHeight\t # nodes = ${nextNodeID - 1}\t root node id [${root.getNodeID}] with childPtrs = ${if(!root.isLeaf) root.map(entry=> entry.asInstanceOf[Rectangle].getChildID) else root.isLeaf}")
    indexfile.updateMetadata(root.getNodeID, treeHeight, nextNodeID-1)
    indexFile.setNumOfPoints(counter)
  }


  private def insertFrom_toReInsert_toInsert(): Unit = {
    /* toReInsert:
     *   1. Οι εγγραφές θα πρέπει να επανεισαχθούν στο επίπεδο από το οποίο βγήκαν
     *        ώστε το δέντρο να είναι ζυγιασμένο (όλα τα φύλλα στο ίδιο επίπεδο)
     *        2.  Αν κατα την διαδικασία επανεισαγωγής το ύψος του δέντρου μεγάλωσε κατα ένα
     *    3. Επανεισηγαγε το πάνω στοιχείο της στοίβας toReInsert
     *
     * toInsert:
     *    4. δεν δημιουργήθηκε νέα ρίζα σε αυτόν τον κύκλο εισαγωγής
     *    5. δεν εκτελέστηκε επανεισαγωγή σε αυτόν τον κύκλο εισαγωγής
     *    6. Εισηγαγε το πάνω στοιχείο της στοίβας toInsert
     *       στα φύλλα αφού είναι point
     *
     * else:
     *    7. δεν υπάρχουν άλλες εγγραφές για εισαγωγή σε αυτόν τον κύκλο, τελος
     */
    var continue = true
    while(continue) {

      if( toReInsert.nonEmpty ){
        val (geoObj: GeometricObject, reInsertLevel: Int) = toReInsert.pop()     /*1*/                                  ; logger.info(s"\n\ttoReInsert contains elements # ${toReInsert.length} \t (toInsert = ${toInsert.size}\t toInclude = ${toInclude.size})")
        val level = reInsertLevel + (if(NewRootMade) 1 else 0)  //2
        toInclude.push((geoObj, level))  //3
        insertFrom_toInclude()
      }

      else if( toInsert.nonEmpty ){
        counter += 1                                                                                                    ; logger.info(s"\n\ttoInsert contains elements # ${toInsert.length} \t (toReInsert = ${toReInsert.size}\t toInclude = ${toInclude.size})")
        NewRootMade = false  //4
        (1 until treeHeight+1).foreach(                //5
            level => reinsertOnLevel.put(level, false))
        toInclude.push((toInsert.pop(), treeHeight))  //6
        insertFrom_toInclude()
      }

      else {//7
        continue = false                                                                                                ; logger.info("\tNo more records. Return from insertPoint")
      }
    }//end while
  }



  private def insertFrom_toInclude(): Unit = {
    /* 1. Φτιάχνει το μονοπάτι στοίβα path από την ρίζα μέχρι επίπεδο εισαγωγής του στοιχείου
     * 2. Το node στο επίπεδο εισαγωγής
     * 3. Εισηγαγε το στοιχείο στο δέντρο στο επιλεγμένο node.
     *
     * isFull:
     *    4. Αν χρειάζεται επανεισαγωγή γύρνα πίσω για να την εκτελέσεις
     *    5. Αλλίως αν έγινε split
     *       6. Αν το split έγινε σε κόμβο όχι ρίζα
     *          Αντικατέστησε το parent MBR του κόμβου που έγινε overflow με τα parent MBRs που προέκυψαν από το split
     *          7. Σβήσε το parent MBR του κόμβου όπου έγινε split
     *       8. Αλλίως αν διασπάστηκε η ρίζα του δέντρου
     *          9. Φτιάξε νέα ρίζα
     *          10. Βάλε στη νέα ρίζα τα MBRs από το split της παλιάς ρίζας
     *
     * adjustPath:
     *    11. Γράψε τον κόμβο όπου έγινε η εισαγωγή του στοιχείου
     *    12. Μέχρι να φτάσεις στη ρίζα
     *        13. Ενημέρωσε το indexfile με τα στοιχεία του parent node μετά το expand του MBR του
     */
    while (toInclude.nonEmpty) {                                                                                        logger.info(s"\n\ttoInclude not empty (# = ${toInclude.size}) : ${toInclude.toString()}")

      var (geoObj, level) = toInclude.pop()
      level += (if(NewRootMade) 1 else 0)
      chooseSubtree(geoObj, level)  //1
      var (currentNode, entryIndex) = path.pop()  /*2*/                                                                 ; logger.info(s"\tgeoObject = ${geoObj.serialize} to be inserted to node [${currentNode.getNodeID}] (isLeaf ${currentNode.isLeaf}) at level = $level ...")
      currentNode.addEntry(geoObj)                /*3*/                                                                 ; logger.info(s"\t\t#entries = ${currentNode.getNumberOfEntries} (${currentNode.getNBytes} bytes)")

      /* Αν με την εισαγωγή ο κόμβος γέμισε -------------------------- */
      if (currentNode.isFull) {
        overflowTreatment(currentNode, path.size + 1)

        if (ReInsert) //4
          return

        if (Split) { //5
          if (path.nonEmpty) { //6 - split non root
            val (currentNode, splitIndex) = path.pop()                                                                  ; logger.info(s"\t\tNon root splitted. Delete entry $splitIndex from current node ${currentNode.serialize}")
            currentNode.deleteEntry(splitIndex)        //7
            indexfile.writeNodeToFile(currentNode)                                                                      ; logger.info(s"\t\tAfter delete : ${currentNode.serialize}")

          } else { // 8 - split root
            root = new NonLeafNode(nextNodeID)  //9
            NewRootMade = true
            treeHeight += 1
            nextNodeID += 1
            /*10*/                                                                                                        logger.info(s"\tAdd geoObject = ${toInclude.top._1} (level = ${toInclude.top._2}) to root")
            root.addEntry(toInclude.pop()._1)                                                                           ; logger.info(s"\tAdd geoObject = ${toInclude.top._1} (level = ${toInclude.top._2}) to root")
            root.addEntry(toInclude.pop()._1)
            indexfile.writeNodeToFile(root)
            indexfile.updateMetadata(root.getNodeID, treeHeight, nextNodeID-1)                                          ; logger.info(s"\tMade new root [${root.getNodeID}] with # entries = ${root.getNumberOfEntries}")
          }
        } //end if Split
      } //end if currentNode.isFull ------------------------------------ */

      /* Αλλίως αν με την εισαγωγή ο κόμβος δεν γέμισε ----------------- */
      else { // ADJUST PATH
        indexfile.writeNodeToFile(currentNode)    /*11*/                                                                ; logger.info(s"\tAdjust path:\t${path.toString()}")
        while (path.nonEmpty) {  //12
          val (parentNode, parentIndex) = path.pop()                                                                    ; logger.info(s"\t\tAdjust parentNode [${parentNode.getNodeID}] parentIndex=$parentIndex : <${parentNode.getEntry(parentIndex)}> with new element at child node entry index = $entryIndex")

          parentNode.getEntry(parentIndex).asInstanceOf[Rectangle]
                    .expandRectangle(currentNode.getEntry(entryIndex))
          currentNode = parentNode
          entryIndex = parentIndex

          indexfile.writeNodeToFile(currentNode)  //13
        }
      }//end not full => adjust path
      /* ----------------------------------------------------------------*/

    }//end while toInclude.nonEmpty
  }


// -------------------------------------------------------------------------------------------------------
  /**  Δημιουργεί το μονοπάτι στοίβα path από την ρίζα έως το επίπεδο εισαγωγής
   * όπου θα εισαχθεί η εγγραφή με γεωμετρικό στοιχείο. Κάθε φορά επιλέγει τον κόμβο
   * που πρέπει να ακολουθήσει η εισαγωγή, αποθηκεύοντας για κάθε κόμβο το δείκτη στον πίνακα
   * entriesOnNode όπου βρίσκεται το MBR που επιλέχθηκε να συμπεριλάβει την εγγραφή.
   * Από εκεί κατεβαίνει στο παιδί του entry και κάνει το ίδιο μέχρι να φτάσει στο
   * επίπεδο εισαγωγής. Εκεί απόθηκεύει τον κόμβο που επιλέχθηκε στο τέλος του μονοπατιού
   *
   * @param geoObj: Το γεωμετρικό στοιχείο της εγγραφής.
   * @param level : Το επίπεδο εισαγωγής (root level == 1 , leaf level == treeHeight)
   */
  private def chooseSubtree(geoObj: GeometricObject, level: Int): Unit = {
    var levelVar = level                                                                                                ; logger.info(s"\tChoose subtree for <${geoObj.serialize}> \t level = $level  height = $treeHeight")
    path = mutable.Stack[(TreeNode, Int)]()
    var current = root  //ξεκίνα από τη ρίζα

    if(treeHeight > 1) {
      while (levelVar > 1) {  //Μέχρι να φτάσεις στο επίπεδο εισαγωγής
        val chosenEntryIndex = current.asInstanceOf[NonLeafNode]
                      // == : τελευταίο επίπεδο πριν τα φύλλα (δείκτες παιδίων δείχνουν σε φύλλα)
                      .chosenEntry(geoObj, path.size == treeHeight-2)

        path.push((current, chosenEntryIndex))                                                                          ; logger.info(s"\t\t> For l = $levelVar node [${current.getNodeID}] (isLeaf ${current.isLeaf}) with # entries = ${current.getNumberOfEntries}. Chosen entry index = $chosenEntryIndex")
        val childID = current.getEntry(chosenEntryIndex).asInstanceOf[Rectangle].getChildID
        //current <- current.load(current.getChild)
        current = indexfile.retrieveNode(childID)
        levelVar -= 1
      }
    }
    //κόμβος στο επίπεδο εισαγωγής. εδώ θα εισαχθεί η εγγραφή
    path.push((current, current.getNumberOfEntries))                                                                    ; logger.info(s"\t\t> For l = $levelVar node [${current.getNodeID}] (isLeaf ${current.isLeaf}) with # entries = ${current.getNumberOfEntries}")
  }

// -------------------------------------------------------------------------------------------------------
  /**  Διαχειρίζεται το overflow
   * @param node  Ο κόμβος του δέντρου που έγινε overflow
   * @param level Το επίπεδο του δέντρου όπου βρίσκεται το node
   *              (root level == 1 , leaf level == treeHeight)
   */
  private def overflowTreatment(node: TreeNode, level: Int): Unit = {
    /* Αν το node είναι ρίζα ή έχει ξαναεκτελεστεί reinsert σε αυτό το επίπεδο
		 * σε αυτόν τον κύκλο εισαγωγής, κάνε split τον κόμβο
		 */
    Split = node.getNodeID == root.getNodeID || reinsertOnLevel(level)                                                  ; logger.info(s"Overflow treatment for node [${node.getNodeID}] at level = $level. isLeaf ${node.isLeaf}")
    ReInsert = !Split
    if(Split)
      split(node, level)
    else{
      reinsertOnLevel.put(level, true)
      reinsert(node, level)
    }
  }


  /** Διαχειρίζεται το split του κόμβου.
   *  - Σπάει τον κόμβο node σε δύο νέους κόμβους node1 (splitted[0]) και
   *    node2 (splitted[1]).
   *  - Αποθηκεύει αυτούς τους νέους κόμβους στο indexfile.
   *  - Τοποθετεί στην λίστα toInclude τα MBR1 (node1) και MBR2 (node2)
   *    για να εισαχθούν στον κόμβο γονέα του node.
   *
   * @param node Ο κόμβος που έγινε overflow.
   * @param level Το επίπεδο του δέντρου όπου βρίσκεται το node που θα διασπαστεί
   *              (root level == 1 , leaf level == treeHeight)
   *              Άρα τα νέα parent MBR θα πρέπει να εισαχθούν σε ένα επίπεδο πάνω από node1, node2
   */
  private def split(node: TreeNode, level: Int): Unit = {
    val nsp: NodeSplit = new NodeSplit(nextNodeID)                                                                      ; logger.info(s"\tSplit node [${node.getNodeID}] with # entries = ${node.getNumberOfEntries}. Is root? ${node.getNodeID == root.getNodeID}")
    nextNodeID += 1
    val (mbr1: Rectangle, node1: TreeNode, mbr2: Rectangle, node2: TreeNode) = nsp.splitNode(node)
    mbr1.setChildID(node1.getNodeID)
    mbr2.setChildID(node2.getNodeID)
    toInclude.push((mbr1, level - 1))
    toInclude.push((mbr2, level - 1))
    indexfile.writeNodeToFile(node1)
    indexfile.writeNodeToFile(node2)                                                                                    ; logger.info(s"\t\t(nodeA [${node1.getNodeID}], #entries : ${node1.getNumberOfEntries} : ${if(!node1.isLeaf) node1.getEntries.map(entry => entry.asInstanceOf[Rectangle].getChildID)}\t, MBRA = ${mbr1.serialize})\n\t\t(nodeB [${node2.getNodeID}], #entries : ${node2.getNumberOfEntries} : ${if(!node2.isLeaf) node2.getEntries.map(entry => entry.asInstanceOf[Rectangle].getChildID)},\t MBRB = ${mbr2.serialize})")
  }


  /**   Ετοιμάζει την λειτουργία reinsert για τον κόμβο.
   *  - Πάρε τον γονέα parent του node
   *  - Πρόσθεσε στην λίστα toReInsert τις εγγραφές που θα βγουν από τον κόμβο για να
   *    γίνουν reinsert
   *  - Σβήσε αυτές τις εγγραφές από τον κόμβο.
   *  - Κάνε adjust το MBR του γονέα στα στοιχεία που απέμειναν στο παιδί
   *  - Ενημέρωσε το indexfile
   *
   * @param node  : Ο κόμβος που έγινε overflow
   * @param level : Το επίπεδο του δέντρου όπου βρίσκεται ο κόμβος
   */
  private def reinsert(node: TreeNode, level: Int): Unit = {
    val (parent: TreeNode, entryIndex: Int) = path.top

    val (updatedNode: TreeNode, updatedParentMBR: Rectangle, entriesToReInsert) =
      new ReInsert(node,                                                  // overflown node
                   parent.getEntry(entryIndex).asInstanceOf[Rectangle],   // parent MBR
                   level).run()                                           // reinsert level
    toReInsert = entriesToReInsert
    parent.updateEntry(entryIndex, updatedParentMBR)
    indexfile.writeNodeToFile(updatedNode)
    indexfile.writeNodeToFile(parent)
  }



  /*
  def checkForDuplicates(node: TreeNode): Unit = {
    if (node.isInstanceOf[NonLeafNode]) {
      val c = mutable.HashMap[Int, Int]()
      node.foreach { r =>
        if (c.contains(r.asInstanceOf[Rectangle].getChildID)) {
          val nodeId = node.getNodeID
        } else
          c.put(r.asInstanceOf[Rectangle].getChildID, 1)
      }
    }
  }
   */


}
