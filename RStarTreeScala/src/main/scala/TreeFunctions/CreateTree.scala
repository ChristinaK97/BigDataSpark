package TreeFunctions

import FileHandler.IndexFile
import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.{LeafNode, NonLeafNode, TreeNode}
import Util.Logger

import scala.collection.mutable

class CreateTree(indexFile: IndexFile, logger_ : Logger) {

  private val logger: Logger = logger_

  private val indexfile: IndexFile = indexFile

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
	* int: index του επιλεγμένου MBR στο entriesOnNode του node, -1 αν TreeNode είναι στο επίπεδο εισαγωγής
  */ private var path: mutable.Stack[(TreeNode, Int)] = _

  /* Εγγραφές που θα εισαχθούν στο node
  */ private val toInclude: mutable.Stack[GeometricObject] = mutable.Stack[GeometricObject]()

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
  def this(indexFile: IndexFile, pointsPartition: Iterator[Point], logger_ : Logger) = {
    this(indexFile, logger_)
    pointsPartition.foreach(point => {
      logger.info(s">> Point $counter : ${point.serialize}")
      toInsert += point
      insertFrom_toReInsert_toInsert()
    })
    indexFile.writeNodeToFile(root)
  }


  private def insertFrom_toReInsert_toInsert(): Unit = {
    var continue = true
    while(continue) {

      if( toReInsert.nonEmpty ){
        logger.info(s"\ttoReInsert contains elements # ${toReInsert.length}")
        val (geoObj: GeometricObject, reInsertLevel: Int) = toReInsert.pop()
        /* Οι εγγραφές θα πρέπει να επανεισαχθούν στο επίπεδο από το οποίο βγήκαν
			   * ώστε το δέντρο να είναι ζυγιασμένο (όλα τα φύλλα στο ίδιο επίπεδο) */
        val level = treeHeight + 1 - reInsertLevel - (if(NewRootMade) 1 else 0)  // Αν κατα την διαδικασία επανεισαγωγής το ύψος του δέντρου μεγάλωσε κατα ένα
        // Επανεισηγαγε το πάνω στοιχείο της στοίβας toReInsert
        toInclude.push(geoObj)
        insertFrom_toInclude(level)
      }

      else if( toInsert.nonEmpty ){
        logger.info(s"\ttoInsert contains elements # ${toInsert.length}")
        counter += 1
        // δεν δημιουργήθηκε νέα ρίζα σε αυτόν τον κύκλο εισαγωγής
        NewRootMade = false
        // δεν εκτελέστηκε επανεισαγωγή σε αυτόν τον κύκλο εισαγωγής
        (1 until treeHeight+1).foreach(
            level => reinsertOnLevel.put(level, false))

        // Εισηγαγε το πάνω στοιχείο της στοίβας toInsert
        toInclude += toInsert.pop()
        insertFrom_toInclude(1)  //στα φύλλα αφού είναι point. Μετράει ανάποδα
      }

      else {//δεν υπάρχουν άλλες εγγραφές για εισαγωγή σε αυτόν τον κύκλο, τελος
        logger.info("\tNo more records. Return from insertPoint")
        continue = false
      }
    }
  }



  private def insertFrom_toInclude(level: Int): Unit = {
    while (toInclude.nonEmpty) {

      val geoObj: GeometricObject = toInclude.pop()
      // Φτιάχνει το μονοπάτι στοίβα path από την ρίζα μέχρι επίπεδο εισαγωγής του στοιχείου
      chooseSubtree(geoObj, level)
      //Το node στο επίπεδο εισαγωγής
      var (currentNode, entryIndex) = path.pop()
      //Εισηγαγε το στοιχείο στο δέντρο στο επιλεγμένο node.
      currentNode.addEntry(geoObj)

      /* Αν με την εισαγωγή ο κόμβος γέμισε -------------------------- */
      if (currentNode.isFull) {
        overflowTreatment(currentNode, path.size + 1)
        //Αν χρειάζεται επανεισαγωγή γύρνα πίσω για να την εκτελέσεις
        if (ReInsert)
          return

        //Αλλίως αν έγινε split
        if (Split) {
          //Αν το split έγινε σε κόμβο όχι ρίζα
          if (path.nonEmpty) {
            //Σβήσε το MBR από το γονέα του splitted node.
            val (currentNode, splitIndex) = path.pop()
            currentNode.deleteEntry(splitIndex)

          //Αλλίως αν διασπάστηκε η ρίζα του δέντρου
          } else {
            //Φτιάξε νέα ρίζα
            NewRootMade = true
            treeHeight += 1
            root = new NonLeafNode(nextNodeID)
            nextNodeID += 1
            indexfile.writeNodeToFile(root)
            indexfile.updateMetadata(root.getNodeID, treeHeight)
            //Βάλε στη νέα ρίζα τα MBRs από το split της παλιάς ρίζας
            toInclude.popAll().foreach(geoObj => root.addEntry(geoObj))
            //Τέλος διαδικασίας εισαγωγής του στοιχείου
          }
        } //end if Split
      } //end if currentNode.isFull ------------------------------------ */

      /* Αλλίως αν με την εισαγωγή ο κόμβος δεν γέμισε ----------------- */
      else { // ADJUST PATH
        while (path.nonEmpty) {  //Μέχρι να φτάσεις στη ρίζα
          val (parentNode, parentIndex) = path.pop()
          // entryIndex != -1 => Κόμβος πιο ψηλά στο δέντρο.  Expand το MBR του γονέα για να συμπεριλάβει το expanded MBR του παιδιού
          // entryIndex == -1 => Γονέας του κόμβου εισαγωγής. Expand το MBR του γονέα για να συμπεριλάβει το στοιχείο
          if (entryIndex == -1)
            entryIndex = currentNode.getNumberOfEntries - 1
          parentNode.getEntry(parentIndex).asInstanceOf[Rectangle]
                    .expandRectangle(currentNode.getEntry(entryIndex))
          currentNode = parentNode

          //Ενημέρωσε το indexfile με τα στοιχεία του parent node μετά το expand του MBR του
          indexfile.writeNodeToFile(currentNode)
        }
      }//end not full => adjust path
      /* ----------------------------------------------------------------*/

    }//end while toInclude.nonEmpty
  }


// -------------------------------------------------------------------------------------------------------
  /**  Δημιουργεί το μονοπάτι στοίβα path από την ρίζα έως το επίπεδο εισαγωγής
   * όπου θα εισαχθεί η εγγραφή με γεωμετρικό στοιχείο O. Κάθε φορά επιλέγει τον κόμβο
   * που πρέπει να ακολουθήσει η εισαγωγή, αποθηκεύοντας για κάθε κόμβο το δείκτη στον πίνακα
   * entriesOnNode όπου βρίσκεται το MBR που επιλέχθηκε να συμπεριλάβει την εγγραφή.
   * Από εκεί κατεβαίνει στο παιδί του entry και κάνει το ίδιο μέχρι να φτάσει στο
   * επίπεδο εισαγωγής. Εκεί απόθηκεύει τον κόμβο που επιλέχθηκε στο τέλος του μονοπατιού,
   * με τιμή entryIndex = -1.
   *
   * @param geoObj     Το γεωμετρικό στοιχείο της εγγραφής.
   * @param level : Το επίπεδο εισαγωγής μετρώντας ανάποδα
   */
  private def chooseSubtree(geoObj: GeometricObject, level: Int): Unit = {
    var levelVar = level
    path = mutable.Stack[(TreeNode, Int)]()
    var current = root  //ξεκίνα από τη ρίζα

    if(treeHeight == 1)      //Η ρίζα είναι ο μοναδικός κόμβος του δέντρου
      path.push((root, -1))  //η εισαγωγή του νέου στοιχείου θα γίνει στη ρίζα

    else {
      //Μέχρι να φτάσεις στο επίπεδο εισαγωγής
      while (levelVar < treeHeight) {
        val chosenEntryIndex = current.asInstanceOf[NonLeafNode]
                      // == : τελευταίο επίπεδο πριν τα φύλλα (δείκτες παιδίων δείχνουν σε φύλλα)
                      .chosenEntry(geoObj, path.size == treeHeight-2)

        path.push((current, chosenEntryIndex))
        val childID = current.getEntry(chosenEntryIndex).asInstanceOf[Rectangle].getChildID
        //current <- current.load(current.getChild)
        current = indexfile.retrieveNode(childID)
        levelVar += 1
      }
      //κόμβος στο επίπεδο εισαγωγής. εδώ θα εισαχθεί η εγγραφή
      path.push((current, -1))
    }
  }

// -------------------------------------------------------------------------------------------------------
  /**  Διαχειρίζεται το overflow
   * @param node  Ο κόμβος του δέντρου που έγινε overflow
   * @param level Το επίπεδο του δέντρου όπου βρίσκεται το node
   *              (Μετρώντας κανονικά - leaf : level==treeHeight)
   */
  private def overflowTreatment(node: TreeNode, level: Int): Unit = {
    /* Αν το node είναι ρίζα ή έχει ξαναεκτελεστεί reinsert σε αυτό το επίπεδο
		 * σε αυτόν τον κύκλο εισαγωγής, κάνε split τον κόμβο
		 */
    Split = node.getNodeID == root.getNodeID || reinsertOnLevel(level)
    ReInsert = !Split
    if(Split)
      split(node)
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
   */
  private def split(node: TreeNode): Unit = {
    //toInclude.clear()
    val nsp: NodeSplit = new NodeSplit(nextNodeID)
    nextNodeID += 1
    val (mbr1: Rectangle, node1: TreeNode, mbr2: Rectangle, node2: TreeNode) = nsp.splitNode(node)
    mbr1.setChildID(node1.getNodeID)
    mbr2.setChildID(node2.getNodeID)
    toInclude.push(mbr1)
    toInclude.push(mbr2)
    indexfile.writeNodeToFile(node1)
    indexfile.writeNodeToFile(node2)
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
   * @param level : Το επίπεδο του δέντρου όπου βρίσκεται ο κόμβος μετρώντας κανονικά.
   */
  private def reinsert(node: TreeNode, level: Int): Unit = {
    val (parent, entryIndex) = path.top
    val ri = new ReInsert(node)
    toReInsert = ri.makeToReInsert(parent.getEntry(entryIndex).asInstanceOf[Rectangle], level)
    ri.updateNode()
    ri.shrinkParent()
    indexfile.writeNodeToFile(node)
    indexfile.writeNodeToFile(parent)
  }



}
