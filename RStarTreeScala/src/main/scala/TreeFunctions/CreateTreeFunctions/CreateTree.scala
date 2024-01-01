package TreeFunctions.CreateTreeFunctions

import FileHandler.IndexFile
import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.{LeafNode, NonLeafNode, TreeNode}
import Util.Constants.DEBUG
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


/* ----------------------------------- INSERT ----------------------------------------------------------------------- */

  /** 1. Καλείται για να δημιουργήσει ένα νέο δέντρο από τις
   * εγγραφές που είναι αποθηκευμένες στο datafile.
   */
  def this(indexFile: IndexFile, pointsPartition: Iterator[Point], logger : Logger) = {
    this(indexFile, logger)
    pointsPartition.foreach(point => {                                                                                    if(DEBUG) logger.info(s"\n>> Point $counter : ${point.serialize}")
      toInsert += point
      insertFrom_toReInsert_toInsert()
    })
    indexFile.writeNodeToFile(root)                                                                                     ; if(DEBUG) logger.info(s"# IOs = ${indexFile.getIOs}\t tree height = $treeHeight\t # nodes = ${nextNodeID - 1}\t root node id [${root.getNodeID}] with childPtrs = ${if(!root.isLeaf) root.map(entry=> entry.asInstanceOf[Rectangle].getChildID) else root.isLeaf}")
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

      if( toReInsert.nonEmpty ){                                                                                          if(DEBUG) logger.info(s"\n\ttoReInsert contains elements # ${toReInsert.length} \t (toInsert = ${toInsert.size}\t toInclude = ${toInclude.size})")
        val (geoObj: GeometricObject, reInsertLevel: Int) = toReInsert.pop()     /*1*/
        val level = reInsertLevel + (if(NewRootMade) 1 else 0)  //2
        toInclude.push((geoObj, level))  //3
        insertFrom_toInclude()
      }

      else if( toInsert.nonEmpty ){
        counter += 1                                                                                                    ; if(DEBUG) logger.info(s"\n\ttoInsert contains elements # ${toInsert.length} \t (toReInsert = ${toReInsert.size}\t toInclude = ${toInclude.size})")
        NewRootMade = false  //4
        (1 until treeHeight+1).foreach(                //5
            level => reinsertOnLevel.put(level, false))
        toInclude.push((toInsert.pop(), treeHeight))  //6
        insertFrom_toInclude()
      }

      else {//7
        continue = false                                                                                                ; if(DEBUG) logger.info("\tNo more records. Return from insertPoint")
      }
    }//end while
  }


  private def insertFrom_toInclude(): Unit = {
    /* 1. Φτιάχνει το μονοπάτι στοίβα path από την ρίζα μέχρι επίπεδο εισαγωγής του στοιχείου
     * 2. Το node στο επίπεδο εισαγωγής
     * 3. Εισηγαγε το στοιχείο στο δέντρο στο επιλεγμένο node
     *    Αυξάνει και το node.SCount κατά 1 αν geoObj isa Point ή κατά geoObj.count αν geoObj isa Rectangle
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
    while (toInclude.nonEmpty) {                                                                                          if(DEBUG) logger.info(s"\n\ttoInclude not empty (# = ${toInclude.size}) : ${toInclude.toString()}")

      var (geoObj, level) = toInclude.pop()
      level += (if(NewRootMade) 1 else 0)
      chooseSubtree(geoObj, level)                  /*1*/
      var (node, chosenIndex) = path.pop()         /*2*/                                                                ; if(DEBUG) logger.info(s"\tgeoObject = ${geoObj.serialize} to be inserted to node [${node.getNodeID}] (isLeaf ${node.isLeaf}) at level = $level ...\t\t ${node.serialize}")
      node.addEntry(geoObj)                       /*3*/                                                                 ; if(DEBUG) logger.info(s"\t\t#entries = ${node.getNumberOfEntries} (${node.getNBytes} bytes)")
                                                                                                                          if(DEBUG) logger.info(s"\t\tUpdated count : [${node.getNodeID}][$chosenIndex].count = ${node.getEntry(chosenIndex).getCount} \t\t [${node.getNodeID}].SCount = ${node.getSCount}")
      /* Αν με την εισαγωγή ο κόμβος γέμισε -------------------------- */
      if (node.isFull) {
        overflowTreatment(node, path.size + 1)

        if (ReInsert) //4
          return

        if (Split) { //5
          if (path.nonEmpty) handleNonRootSplit()   //6 - split non root
          else handleRootSplit()                   // 8 - split root
        }
      }/* -------------------------------------------------------------- */

      /* Αλλίως αν με την εισαγωγή ο κόμβος δεν γέμισε ----------------- */
      else { // ADJUST PATH
        indexfile.writeNodeToFile(node)           /*11*/                                                                ; if(DEBUG) logger.info(s"\tAdjust path:\t${path.map{case (node, i) => (node.getNodeID, i)}}")
        while (path.nonEmpty) {                  /*12*/
          val childEntry = node.getEntry(chosenIndex)
          val popped = path.pop()
          node = popped._1                     // parent node
          chosenIndex = popped._2             // index of MBR in parent node
                                                                                                                        ; if(DEBUG) logger.info(s"\t\tAdjust parentNode [${node.getNodeID}] parentIndex=$chosenIndex : <${node.getEntry(chosenIndex)}> with new element at child node entry index = $chosenIndex \t\t ${node.serialize}")
          node.getEntry(chosenIndex).expandRectangle(childEntry)
          node.getEntry(chosenIndex).increaseCount(geoObj)
          node.increaseSCount(geoObj)                                                                                   ; if(DEBUG) logger.info(s"\t\tUpdated count : [${node.getNodeID}][$chosenIndex].count = ${node.getEntry(chosenIndex).getCount} \t\t [${node.getNodeID}].SCount = ${node.getSCount}")

          indexfile.writeNodeToFile(node)  //13
        }
      }/* ----------------------------------------------------------------*/

    }//end while, toInclude is now empty
  }


/* ----------------------------------- CHOOSE SUBTREE ------------------------------------------------------------- */

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
    var levelVar = level                                                                                                ; if(DEBUG) logger.info(s"\tChoose subtree for <${geoObj.serialize}> \t level = $level  height = $treeHeight")
    path = mutable.Stack[(TreeNode, Int)]()
    var current = root  //ξεκίνα από τη ρίζα

    if(treeHeight > 1) {
      while (levelVar > 1) {  //Μέχρι να φτάσεις στο επίπεδο εισαγωγής
        val chosenEntryIndex = current.asInstanceOf[NonLeafNode]
                      // == : τελευταίο επίπεδο πριν τα φύλλα (δείκτες παιδίων δείχνουν σε φύλλα)
                      .chosenEntry(geoObj, path.size == treeHeight-2)

        path.push((current, chosenEntryIndex))                                                                          ; if(DEBUG) logger.info(s"\t\t> For l = $levelVar node [${current.getNodeID}] -> $chosenEntryIndex \t isLeaf ${current.isLeaf} \t # entries = ${current.getNumberOfEntries}")
        val childID = current.getEntry(chosenEntryIndex).asInstanceOf[Rectangle].getChildID
        //current <- current.load(current.getChild)
        current = indexfile.retrieveNode(childID)
        levelVar -= 1
      }
    }
    //κόμβος στο επίπεδο εισαγωγής. εδώ θα εισαχθεί η εγγραφή
    path.push((current, current.getNumberOfEntries))                                                                    ; if(DEBUG) logger.info(s"\t\t> For l = $levelVar node [${current.getNodeID}] -> ${current.getNumberOfEntries} \t isLeaf ${current.isLeaf} \t # entries = ${current.getNumberOfEntries}")
  }


/* --------------------------------- OVERFLOW TREATMENT ------------------------------------------------------------- */
  /**  Διαχειρίζεται το overflow
   *   Αν το node είναι ρίζα ή έχει ξαναεκτελεστεί reinsert σε αυτό το επίπεδο
   *   σε αυτόν τον κύκλο εισαγωγής, κάνε split τον κόμβο
   * @param node  Ο κόμβος του δέντρου που έγινε overflow
   * @param level Το επίπεδο του δέντρου όπου βρίσκεται το node
   *              (root level == 1 , leaf level == treeHeight)
   */
  private def overflowTreatment(node: TreeNode, level: Int): Unit = {

    Split = node.getNodeID == root.getNodeID || reinsertOnLevel(level)                                                  ; if(DEBUG) logger.info(s"\nOverflow treatment for node [${node.getNodeID}] at level = $level. isLeaf ${node.isLeaf}\t\t ${node.serialize}")
    ReInsert = !Split
    if(Split)
      split(node, level)
    else{
      reinsertOnLevel.put(level, true)
      reinsert(node, level)
    }
  }

/* --------------------------------  1. REINSERT  ------------------------------------------------------------------ */
  /** Ετοιμάζει την λειτουργία reinsert για τον κόμβο.
   *  - Πάρε τον γονέα parent του node
   *  - Πρόσθεσε στην λίστα toReInsert τις εγγραφές που θα βγουν από τον κόμβο για να
   *    γίνουν reinsert
   *  - Σβήσε αυτές τις εγγραφές από τον κόμβο.
   *  - Κάνε adjust το MBR του γονέα στα στοιχεία που απέμειναν στο παιδί
   *  - Ενημέρωσε τους aggregation counters στους nodes του path ότι αφαιρέθηκαν στοιχεία
   *  - Ενημέρωσε το indexfile
   *
   * @param node  : Ο κόμβος που έγινε overflow
   * @param level : Το επίπεδο του δέντρου όπου βρίσκεται ο κόμβος
   */
  private def reinsert(node: TreeNode, level: Int): Unit = {
    val (directParent: TreeNode, chosenIndex: Int) = path.pop()                                                         ; if(DEBUG) logger.info(s"\tReInsert node [${node.getNodeID}] with # entries = ${node.getNumberOfEntries}. Is root? ${node.getNodeID == root.getNodeID} . [${node.getNodeID}].SCount = ${node.getSCount}  \t\t ${node.serialize}")

    val (updatedNode: TreeNode, updatedParentMBR: Rectangle, entriesToReInsert, nRemovedPoints) =
      new ReInsert(node,                                                        // overflown node
                   directParent.getEntry(chosenIndex).asInstanceOf[Rectangle], // parent MBR
                   level).run()                                               // reinsert level

    toReInsert.addAll(entriesToReInsert)
    directParent.updateEntry(chosenIndex, updatedParentMBR)
    directParent.decreaseSCount(nRemovedPoints)                                                                         ; if(DEBUG) logger.info(s"\t\tUpdated count : [${directParent.getNodeID}][$chosenIndex].count = ${directParent.getEntry(chosenIndex).getCount} \t\t [${directParent.getNodeID}].SCount = ${directParent.getSCount} \t\t ${directParent.serialize}")
    indexfile.writeNodeToFile(updatedNode)
    indexfile.writeNodeToFile(directParent)

    while (path.nonEmpty) {
      val (parentNode: TreeNode, chosenIndex: Int) = path.pop()
      parentNode.getEntry(chosenIndex).decreaseCount(nRemovedPoints)
      parentNode.decreaseSCount(nRemovedPoints)
      indexfile.writeNodeToFile(parentNode)                                                                             ; if(DEBUG) logger.info(s"\t\tUpdated count : [${parentNode.getNodeID}][$chosenIndex].count = ${parentNode.getEntry(chosenIndex).getCount} \t\t [${parentNode.getNodeID}].SCount = ${parentNode.getSCount} \t\t ${parentNode.serialize}")
    }
  }



/* ------------------------------------  2. SPLIT  ------------------------------------------------------------------ */
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
    val nsp: NodeSplit = new NodeSplit(nextNodeID)                                                                      ; if(DEBUG) logger.info(s"\tSplit node [${node.getNodeID}] \t # entries = ${node.getNumberOfEntries} \t [${node.getNodeID}].SCount = ${node.getSCount} \t  Is root? ${node.getNodeID == root.getNodeID} \t\t ${node.serialize}")
    nextNodeID += 1
    val (mbr1: Rectangle, node1: TreeNode, mbr2: Rectangle, node2: TreeNode) = nsp.splitNode(node)

    mbr1.setChildID(node1.getNodeID)
    mbr1.setCount(node1.getSCount)
    toInclude.push((mbr1, level - 1))
    indexfile.writeNodeToFile(node1)

    mbr2.setChildID(node2.getNodeID)
    mbr2.setCount(node2.getSCount)
    toInclude.push((mbr2, level - 1))
    indexfile.writeNodeToFile(node2)                                                                                    ; if(DEBUG) logger.info(s"\t\t(nodeA [${node1.getNodeID}] \t [${node1.getNodeID}].SCount = ${node1.getSCount} \t #entries : ${node1.getNumberOfEntries} : ${if(!node1.isLeaf) node1.getEntries.map(entry => entry.asInstanceOf[Rectangle].getChildID)}\t, MBRA = ${mbr1.serialize}\t\t ${node1.serialize})\n\t\t(nodeB [${node2.getNodeID}] \t [${node2.getNodeID}].SCount = ${node2.getSCount} \t #entries : ${node2.getNumberOfEntries} : ${if(!node2.isLeaf) node2.getEntries.map(entry => entry.asInstanceOf[Rectangle].getChildID)},\t MBRB = ${mbr2.serialize}) \t\t${node2.serialize}")
  }


  /** Διαχειρίζεται η διάσπαση της ρίζας του δέντρου
   * 9. Φτιάξε νέα ρίζα
   * 10. Βάλε στη νέα ρίζα τα MBRs από το split της παλιάς ρίζας
   * Αυξάνει το root.SCount κατά mbr1.SCount και mbr2.SCount
   */
  private def handleRootSplit(): Unit = {
    root = new NonLeafNode(nextNodeID) //9
    NewRootMade = true
    treeHeight += 1
    nextNodeID += 1
    /*10*/                                                                                                                if(DEBUG) logger.info(s"\tAdd geoObject = ${toInclude.top._1} (level = ${toInclude.top._2}) to root")
    root.addEntry(toInclude.pop()._1)                                                                                   ; if(DEBUG) logger.info(s"\tAdd geoObject = ${toInclude.top._1} (level = ${toInclude.top._2}) to root")
    root.addEntry(toInclude.pop()._1)
    indexfile.writeNodeToFile(root)
    indexfile.updateMetadata(root.getNodeID, treeHeight, nextNodeID - 1)                                                ; if(DEBUG) logger.info(s"\tMade new root [${root.getNodeID}] with # entries = ${root.getNumberOfEntries} \t Updated count: [${root.getNodeID}].SCount = ${root.getSCount}  \t\t ${root.serialize}")
  }



  /** Διαχειρίζεται την διάσπαση κόμβων οι οποίοι δεν είναι η ρίζα του δέντρου.
   *  1. Το πρώτο στοιχείο στο path είναι ο άμεσος γονέας του κόμβου που διασπάστηκε
   *     -> parentNode[chosenIndex] είναι το MBR που περικλύει τα στοιχεία του
   *     διασπασμένου κόμβου : θα αντικατασταθεί από τα mbr1 και mbr2
   *     άρα διέγραψε το από τα entries του parent node
   *     2. Μειώνει τους counters στα υπόλοιπα nodes του path κατά MBR.count
   *     αφού το MBR αφαιρέθηκε από τον direct parent node. Όταν θα εισαχθούν τα
   *     mbr1 και mbr2 τα count τους θα προστεθούν στους ίδιους counters
   *     για να επανέλθει συνέπεια.
   *     Μέχρι τη ρίζα, μειώνονται το parentNode.SCount και το
   *     chosenEntry.count του parentNode (parentNode[chosenIndex].count)
   */
  private def handleNonRootSplit(): Unit = {
    var nRemovedPoints: Int = -1
    while (path.nonEmpty) {
      val (parentNode, chosenIndex) = path.pop()
      val chosenEntry = parentNode.getEntry(chosenIndex)

      if (nRemovedPoints == -1) {
        nRemovedPoints = chosenEntry.getCount                                                                           ; if(DEBUG) logger.info(s"\t\tNon root splitted. Delete entry $chosenIndex from current node ${parentNode.serialize}\n\t\tnRemovedPoints = $nRemovedPoints")
        parentNode.deleteEntry(chosenIndex) //1
      } else
        chosenEntry.decreaseCount(nRemovedPoints) //2
      parentNode.decreaseSCount(nRemovedPoints)                                                                         ; if(DEBUG) if(chosenIndex<parentNode.getNumberOfEntries) logger.info(s"\t\tUpdated count : [${parentNode.getNodeID}][$chosenIndex].count = ${parentNode.getEntry(chosenIndex).getCount} \t\t [${parentNode.getNodeID}].SCount = ${parentNode.getSCount}")
      indexfile.writeNodeToFile(parentNode)
    }
  }






}
