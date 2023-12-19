package TreeStructure

import Geometry.{GeometricObject, Rectangle}

import scala.collection.mutable

class NonLeafNode(nodeId: Int) extends TreeNode(nodeId) {

  override def isLeaf: Boolean = false

  /** @param index : Η θέση του entriesOnNode
   * @return Το ορθογώνιο (Rectangle) της εγγραφής στη θέση index του entriesOnNode */
  def getRectangle(index: Int): Rectangle =
    super.getEntry(index).asInstanceOf[Rectangle]

/* ---------------------Για την chooseSubtree-----------------------------------------------*/

  private var chosenRectangles: mutable.HashMap[Int, Rectangle] = _


  def chosenEntry(O: GeometricObject, childPointersToLeaf: Boolean): Int = {
    chosenRectangles = new mutable.HashMap[Int, Rectangle]()
    /* Για κάθε ορθογώνιο στο Node (entriesOnNode):
	   *     Δημιουργεί αντίγραφο
		 *     Επέκτεινει το αντίγραφο για να συμπεριλάβει το Point P
		 *     Τοποθετεί το αντίγραφο στο chosenRectangles ως υποψήφιο
	   */
    this.zipWithIndex.foreach{case (rectangle: Rectangle, i) =>
      chosenRectangles += (i -> rectangle.makeCopy.expandRectangle(O))
    }
    var chosenEntry: Int = -1

    //Αν οι childPointers του Node δείχνουν σε φύλλα επιλογή
    //με βάση και τα τρία κριτήρια. Αλλιώς με τα Κ2 και Κ3
    if(childPointersToLeaf) {
      /* Κριτήριο 1 */
      chosenEntry = minOverlapEnlargement
      if(chosenEntry != -1)    //αν μόνο ένα ορθογώνιο ικανοποιεί το κριτήριο
        return chosenEntry     //επέστρεψε τη θέση του μέσα στο entriesOnNode
    }

    /* Κριτήριο 2 */
    chosenEntry = minAreaEnlargement
    if(chosenEntry != -1)
      return chosenEntry

    /* Κριτήριο 3 */
    chosenEntry = minArea
    if (chosenEntry != -1)
      return chosenEntry

    chosenEntry  //σφάλμα. δεν πρέπει να επιστρέφει ποτέ -1.
  }


  /** ΚΡΙΤΗΡΙΟ 1: Επέλεξε την εγγραφή του Node της οποίας το ορθογώνιο
   * απαιτεί την ελάχιστη αύξηση του εμβαδού της επικάλυψης για να
   * συμπεριλάβει τα νέα δεδομένα
   *
   * @return Το δείκτη στον entriesOnNode όπου είναι αποθηκευμένο το MBR
   *         που επιλέχθηκε για να γίνει η εισαγωγή του αντικειμένου.
   */
  private def minOverlapEnlargement: Int = {
    val evaluation = mutable.HashMap[Int, Double]()

    //για κάθε υποψήφιο ορθογώνιο i
    chosenRectangles.foreach { case (i, expandedR_i) => {
      //συνολική αύξηση του εμβαδού της επικάληψης που προκάλεσαι η επέκταση
      var SOverlapArea: Double = 0.0

      //για κάθε άλλο ορθογώνιο σε αυτο το node/ j!=i
      for (j <- chosenRectangles.keys if j != i) {
        /* Αύξηση του εμβαδού της επικάλυψης μεταξύ i και j =
				 *   εμβαδόν της επικάλυψης μεταξύ i και j μετά την επέκταση
				 * - εμβαδόν της επικάλυψης μεταξύ i και j πριν την επέκταση
				 */
        SOverlapArea += (
            expandedR_i.overlapArea(getRectangle(j))
          - getRectangle(i).overlapArea(getRectangle(j))
        )
      }
      evaluation += (i -> SOverlapArea)
    }}
    choose(evaluation)
  }


  /** ΚΡΙΤΗΡΙΟ 2: Επέλεξε την εγγραφή του Node της οποίας το ορθογώνιο
   * απαιτεί την ελάχιστη αύξηση του εμβαδού του για να συμπεριλάβει τα νέα δεδομένα
   *
   * @return Το δείκτη στον entriesOnNode όπου είναι αποθηκευμένο το MBR
   *         που επιλέχθηκε για να γίνει η εισαγωγή του αντικειμένου.
   */
  private def minAreaEnlargement: Int = {
    val evaluation = mutable.HashMap[Int, Double]()
    //για κάθε υποψήφιο ορθογώνιο i
    chosenRectangles.foreach { case (i, expandedR_i) =>
      /* Αύξηση του εμβαδού του i =
			 *   εμβαδόν του i μετά την επέκταση
			 * - εμβαδόν του i πριν την επέκταση
			*/
      evaluation += (i ->
        (expandedR_i.getArea - getRectangle(i).getArea)
      )
    }
    choose(evaluation)
  }


  /** ΚΡΙΤΗΡΙΟ 3: Επέλεξε την εγγραφή του Node της οποίας το ορθογώνιο
   * έχει το μικρότερο εμβαδόν.
   *
   * @return Το δείκτη στον entriesOnNode όπου είναι αποθηκευμένο το MBR
   *         που επιλέχθηκε για να γίνει η εισαγωγή του αντικειμένου.
   */
  private def minArea: Int = {
    val evaluation = new mutable.HashMap[Int, Double]
    //για κάθε υποψήφιο ορθογώνιο i
    this.zipWithIndex.foreach{case (rectangle: Rectangle, i) =>
      evaluation += (i -> rectangle.getArea)
    }
    choose(evaluation)
  }


  /** Καλείται από κάθε κριτήριο.
   *
   * @param evaluation : Η τιμή κάθε ορθογωνίου στο εκάστοτε κριτήριο
   *                   (K1.Overlap Area Enlargement, K2.Area Enlargement, K3.Area)
   * @return Αν από το εκάστοτε κριτήριο προέκυψε μόνο ένα υποψήφιο
   *         ορθογώνιο, τη θέση του στο entriesOnNode
   *         Αλλιώς αν παραπάνω από ένα υποψήφια, επιστρέφει null.
   */
  private def choose(evaluation: mutable.HashMap[Int,Double]): Int = {
    val minValue: Double = evaluation.values.min

    /* για κάθε υποψήφιο ορθογώνιο
     * αν το overlap που προκάλεσαι είναι μεγαλύτερο από το min
     * μεσα σε κάποιο threshold, αφαίρεσαι το από τα υποφήφια */
    chosenRectangles = chosenRectangles.filter { case (i, _) =>
      val diffPercentage =
        if(evaluation(i) != 0.0) (evaluation(i) - minValue) / evaluation(i)
        else 0.0
      diffPercentage <= 0.2
    }
    //αν υπάρχει μόνο ένα υποψήφιο ορθωγώνιο
    if(chosenRectangles.size == 1) chosenRectangles.head._1
    // αν υπάρχουν παραπάνω από ένα υποψήφια
    else -1
  }

}
