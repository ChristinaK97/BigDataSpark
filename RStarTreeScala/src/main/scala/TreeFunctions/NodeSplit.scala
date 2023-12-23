package TreeFunctions

import Geometry.Rectangle
import TreeStructure.{LeafNode, NonLeafNode, TreeNode}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class NodeSplit(nextId: Int) {

  //TODO get from main
  private val N: Int = 5

  private var splitIndex: Int = _
  private val nextID: Int = nextId

  def splitNode(nodeToSplit: TreeNode): (Rectangle, TreeNode, Rectangle, TreeNode) = {
    splitIndex = (nodeToSplit.getNumberOfEntries - 1) / 2
    nodeToSplit match {
      case leafNode:    LeafNode    => splitNode(leafNode)
      case nonLeafNode: NonLeafNode => splitNode(nonLeafNode)
    }
  }



/* ============================ Split Leaf Node ========================================= */

  /** Διασπά έναν overflown κόμβο φύλλο σε δύο νέα φύλλα, όπου το καθένα θα έχει το 50% των
   *  Points του αρχικού κόμβου.
   *
   * @param node: Το φύλλο που θα διασπαστεί. Σε αυτό έχουν ενταχτεί μία λίστα Points.
   * @return Tuple[Rectangle, LeafNode, Rectangle, LeafNode]
   *         - MBR1 : To Rectangle που περικλύει τα σημεία του πρώτου φύλλου. Θα πρέπει να
   *            ενταχτεί σε ένα NonLeafNode στο προτελευταίο επίπεδο του δέντρου και ο childptr
   *            του να δείχνει στο 1ο φύλλο (=nodeID).
   *         - leafNode1 : Ο 1ος κόμβος φύλλο που περιέχει το 50% των σημείων [0, splitIndex),
   *           έχει nodeID και είναι παιδί του MBR1.
   *         - MBR2 : To Rectangle που περικλύει τα σημεία του δεύτερου φύλλου. Θα πρέπει να
   *            ενταχτεί σε ένα NonLeafNode στο προτελευταίο επίπεδο του δέντρου και ο childptr
   *            του να δείχνει στο 2ο φύλλο (=nextID).
   *         - leafNode1 : Ο 1ος κόμβος φύλλο που περιέχει το υπόλοιπο 50% των σημείων [splitIndex, last],
   *           έχει nextID και είναι παιδί του MBR2.
   */
  private def splitNode(node: LeafNode): (Rectangle, LeafNode, Rectangle, LeafNode) = {
    /* Πίνακας λιστών με τις εγγραφές του κόμβου ταξινομημένες με
		 * βάση τον άξονα axis */
    val sorts =
      Vector.tabulate(N)(axis => node.sortEntriesBy(axis))

    /* Τα MBRs που περιέχουν τις ταξινομημένες εγγραφές σύμφωνα με κάθε άξονα.
		 * Για axis=0 R1 : MBRs[0] , R2: MBRs[1]
		 * Γενικά: Για axis R1 : MBRs[2*axis] , R2: MBRs[2*axis+1]
		 */
    val MBRs = ListBuffer[Rectangle]()

    //Για κάθε άξονα axis
    sorts.foreach{ axisSort => {

     //Πρώτο group από εγγραφές
      //Δημιούργησε το R1 που θα περιέχει αυτά τα σημεία [0, splitIndex)
      val R1 = new Rectangle(axisSort.head)
      for(entry <- 1 until splitIndex)
        R1.expandRectangle(axisSort(entry))

     //Δεύτερο group από εγγραφές
      //Δημιούργησε το R2 που θα περιέχει αυτά τα σημεία [splitIndex, last]
      val R2 = new Rectangle(axisSort(splitIndex + 1))
      for (entry <- splitIndex + 2 until axisSort.length)
        R2.expandRectangle(axisSort(entry))

      //Αποθήκευσε αυτά τα R1 και R2 για την ταξινόμηση με βάση τον άξονα axis.
      MBRs += R1
      MBRs += R2
    }}

    /* ***** CHOOSE AXIS ****** */
    //1ο κριτήριο : Ελάχιστο overlap μεταξύ των R1 και R2
    val evaluation = mutable.HashMap[Int, Double]()
    for(axis <- 0 until N)
      evaluation += (axis ->
        MBRs(2*axis).overlapArea( MBRs(2*axis+1)) )

    var chosenAxis = -1
    val candidates: Iterable[Int] = chooseAxis(evaluation)
    if(candidates.size == 1)
      chosenAxis = candidates.head

    else {
      //Ισοβαθμία στο 1ο κριτήριο
      //2ο κριτήριο: Ελάχιστο εμβαδόν των R1 και R2
      evaluation.clear()

      candidates.foreach(axis =>
        evaluation += (axis -> (
          MBRs(2*axis).getArea + MBRs(2*axis+1).getArea))
      )
      chosenAxis = chooseAxis(evaluation).head
    }
   /* ******** */
    (
      MBRs(2*chosenAxis),
      new LeafNode(node.getNodeID, sorts(chosenAxis).take(splitIndex)),
      MBRs(2*chosenAxis+1),
      new LeafNode(nextID, sorts(chosenAxis).drop(splitIndex))
    )
  }


  private def chooseAxis(evaluation: mutable.HashMap[Int, Double]): Iterable[Int] = {
    val minValue: Double = evaluation.values.min
    evaluation.filter { case (i, _) =>
      val diffPercentage =
        if (evaluation(i) != 0.0) (evaluation(i) - minValue) / evaluation(i)
        else 0.0
      diffPercentage <= 0.2
    }.keys
  }



/* ============================ Split Non Leaf Node ========================================= */

  /** Διασπά έναν overflown non-leaf κόμβο σε δύο νέα non-leaves, όπου το καθένα θα έχει το 50% των
   * Rectangles του αρχικού κόμβου.
   *
   * @param node : Το non-leaf node που θα διασπαστεί. Σε αυτό έχουν ενταχτεί μία λίστα Rectangles.
   * @return Tuple[Rectangle, NonLeafNode, Rectangle, NonLeafNode]
   *         - MBR1 : To Rectangle που περικλύει τα Rectangles του πρώτου non-leaf. Θα πρέπει να
   *           ενταχτεί σε ένα NonLeafNode στο προτελευταίο επίπεδο του δέντρου και ο childptr
   *           του να δείχνει στο 1ο non-leaf (=nodeID).
   *         - nonLeafNode1 : Ο 1ος non-leaf κόμβος που περιέχει το 50% των Rectangles [0, splitIndex),
   *           έχει nodeID και είναι παιδί του MBR1.
   *         - MBR2 : To Rectangle που περικλύει τα Rectangles του δεύτερου non-leaf. Θα πρέπει να
   *           ενταχτεί σε ένα NonLeafNode στο προτελευταίο επίπεδο του δέντρου και ο childptr
   *           του να δείχνει στο 2ο non-leaf (=nextID).
   *         - nonLeafNode1 : Ο 1ος non-leaf κόμβος που περιέχει το υπόλοιπο 50% των Rectangles [splitIndex, last],
   *           έχει nextID και είναι παιδί του MBR2.
   */
  private def splitNode(node: NonLeafNode): (Rectangle, NonLeafNode, Rectangle, NonLeafNode) = {

    val sorts = ListBuffer[ListBuffer[Rectangle]]()
    //για κάθε άξονα δημιούργησε τις δύο ταξινομήσεις ως προς pm και pM
    for(axis:Int <- 0 until N) {
      sorts += node.sortEntriesBy(axis, "pm")
      sorts += node.sortEntriesBy(axis, "pM")
    }

    /* Άξονας : axis
     * Ταξινόμηση σύμφωνα με το σημείο:
     * 		pm : sorts[diamerisi=2*axis]
     * 		pM : sorts[diamerisi=2*axis+1]
     * Ορθογώνια που περιέχουν τα σημεία
     * 		R1: MBRs(diamerisi)[0]
     * 		R2: MBRs(diamerisi)[1]
     */
    val MBRs = ListBuffer[Array[Rectangle]]()
    // για κάθε διαμέριση : βρες τα MBR των δυο group
    sorts.foreach { diamerisi: ListBuffer[Rectangle] => {

      //Πρώτο γκρουπ εγγραφών που περιέχονται από το R1
      val R1 = diamerisi.head.makeCopy
      for(entry <- 0 until splitIndex)
        R1.expandRectangle(diamerisi(entry))

      //Δεύτερο γκρουπ εγγραφών που περιέχονται από το R2
      val R2 = diamerisi(splitIndex+1).makeCopy
      for(entry <- splitIndex+1 until diamerisi.length)
        R2.expandRectangle(diamerisi(entry))

      MBRs += Array(R1, R2)
    }}

    /* ***** CHOOSE AXIS ****** */
    // Κριτήριο: Ελάχιστη συνολική περίμετρος των R1 και R2
    var minPerimeter: Double = Double.MaxValue
    var chosenAxis: Int = -1
    for(axis <- 0 until N) {
      val SPerimeter =
        MBRs(2*axis)(0).getPerimeter +    // R1.pm perimeter
        MBRs(2*axis)(1).getPerimeter +    // R2.pm perimeter
        MBRs(2*axis+1)(0).getPerimeter +  // R1.pM perimeter
        MBRs(2*axis+1)(1).getPerimeter    // R2.pM perimeter

      if(SPerimeter < minPerimeter) {
        minPerimeter = SPerimeter
        chosenAxis = axis
      }
    }

    /* ****** ΔΙΑΛΕΞΕ ΔΙΑΜΕΡΙΣΗ ΤΟΥ CHOSEN AXIS ****** */
    var chosenDiamerisi = -1

    // ΚΡΙΤΗΡΙΟ 1 Τη διαμεριση που προκαλει τη μικροτερη overlap area μεταξυ των R1 και R2
    val overlapFor_pm = MBRs(2*chosenAxis)(0)  .overlapArea( MBRs(2*chosenAxis)(1) )
    val overlapFor_pM = MBRs(2*chosenAxis+1)(0).overlapArea( MBRs(2*chosenAxis+1)(1) )

    if(overlapFor_pm < overlapFor_pM)
      chosenDiamerisi = 2*chosenAxis
    else if(overlapFor_pM < overlapFor_pm)
      chosenDiamerisi = 2*chosenAxis+1
    else {
      //ισοδυναμια στο πρώτο κριτήριο
      /* ΚΡΙΤΗΡΙΟ 2 Τη διαμέριση που τα R1 και R2 έχουν το μικρότερο εμβαδόν */
      val areaFor_pm = MBRs(2*chosenAxis)(0).getArea   + MBRs(2*chosenAxis)(1).getArea
      val areaFor_pM = MBRs(2*chosenAxis+1)(0).getArea + MBRs(2*chosenAxis+1)(1).getArea

      if(areaFor_pm < areaFor_pM)
        chosenDiamerisi = 2*chosenAxis
      else if(areaFor_pM < areaFor_pm)
        chosenDiamerisi = 2*chosenAxis+1
      else
        chosenDiamerisi = 2*chosenAxis
    }
    /* ******** */
    (
      MBRs(2 * chosenDiamerisi)(0),
      new NonLeafNode(node.getNodeID, sorts(chosenDiamerisi).take(splitIndex)), //Πρώτος νέος κόμβος
      MBRs(2 * chosenDiamerisi + 1)(1),
      new NonLeafNode(nextID, sorts(chosenDiamerisi).drop(splitIndex))          //Δεύτερος νέος κόμβος
    )
  }


}
