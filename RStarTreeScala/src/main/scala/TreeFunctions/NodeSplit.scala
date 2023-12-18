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
   * @return Tuple[Rectangle, Point, Rectangle, Point]
   *         - MBR1 : To Rectangle που περικλύει τα σημεία του πρώτου φύλλου. Θα πρέπει να
   *            ενταχτεί σε ένα NonLeafNode στο προτελευταίο επίπεδο του δέντρου και ο childptr
   *            του να δείχνει στο 1ο φύλλο (=nodeID).
   *         - leafNode1 : Ο 1ος κόμβος φύλλο που περιέχει το 50% των σημείων, έχει nodeID και
   *            είναι παιδί του MBR1.
   *         - MBR2 : To Rectangle που περικλύει τα σημεία του δεύτερου φύλλου. Θα πρέπει να
   *            ενταχτεί σε ένα NonLeafNode στο προτελευταίο επίπεδο του δέντρου και ο childptr
   *            του να δείχνει στο 2ο φύλλο (=nextID).
   *         - leafNode1 : Ο 1ος κόμβος φύλλο που περιέχει το υπόλοιπο 50% των σημείων, έχει nextID
   *            και είναι παιδί του MBR2.
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
      //Δημιούργησε το R1 που θα περιέχει αυτά τα σημεία
      val R1 = new Rectangle(axisSort.head)
      for(entry <- 1 until splitIndex)
        R1.expandRectangle(axisSort(entry))

     //Δεύτερο group από εγγραφές
      //Δημιούργησε το R2 που θα περιέχει αυτά τα σημεία
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
      new LeafNode(node.getNodeID, sorts(chosenAxis).take(splitIndex+1)),
      MBRs(2*chosenAxis+1),
      new LeafNode(nextID, sorts(chosenAxis).drop(splitIndex+1))
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

  private def splitNode(node: NonLeafNode): (Rectangle, NonLeafNode, Rectangle, NonLeafNode) = {
    (null, null, null, null)
  }





}
