package TreeFunctions.CreateTreeFunctions

import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.TreeNode

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Ετοιμάζει μια λίστα με τα στοιχεία (εγγραφές) που πρέπει να αφαιρεθούν
 * από το node και να γίνουν reinsert στο δέντρο. (λίστα toReInsert)
 *
 * @param node : To TreeNode όπου προέκυψε overflow
 * @param MBR  : Το Parent Rectangle MBR που περιέχει τις εγγραφές του node
 * @param reInsertLevel : Το επίπεδο του δέντρου όπου ανήκει ο overflown node.
 *                        Εκεί θα πρέπει να επανεισαχθούν οι εγγραφές
 */
class ReInsert(node: TreeNode, MBR: Rectangle, reInsertLevel: Int) {


  /**
   * @return Tuple:
   *  - TreeNode: Ο overflown κόμβος ενημερωμένος
   *  - Rectangle: Το parent MBR που περικλύει τις εγγραφές που απέμειναν στο node
   *  - ListBuffer[(GeometricObject, Int)]. Το πεδίο GeometricObject είναι
   *    - Rectangle αν node isa NonLeafNode
   *    - Point αν node isa LeafNode
   */
  def run():
      (TreeNode, Rectangle, mutable.Stack[(GeometricObject, Int)]) =
  {
    val toReInsert: mutable.Stack[(GeometricObject, Int)] = mutable.Stack[(GeometricObject, Int)]()

    // Ταξινόμηση των αντικειμένων με βάση την απόσταση τους από το κέντρο του parent MBR (desc)
    val entrySDistance =
      node.getEntries.zipWithIndex.map { case (entry, entryIndex) =>
            ( entryIndex, MBR.distanceFromCenter(entry) )
      }
      .sortBy(_._2).reverse

    //το 30% των εγγραφών θα επανεισαχθεί
    val splitIndex = (node.getNumberOfEntries * 0.3).toInt
    val remainingEntries = ListBuffer[GeometricObject]()

    entrySDistance.zipWithIndex.foreach{case ((entryIndex, _), i) =>  // i is a counter: until 30% is reached
      if(i <= splitIndex)
        // το 30% των εγγραφών με την μεγαλύτερη απόσταση από το κέντρο
        toReInsert += ((node.getEntry(entryIndex), reInsertLevel))
      else
        // οι υπόλοιπες (70%) εγγραφές που είναι πιο κοντά στο κέντρο, θα διατηρηθούν
        remainingEntries += node.getEntry(entryIndex)
    }
    node.setEntries(remainingEntries)
    (node, shrinkParent(), toReInsert)
  }


  /** Συρρικνώνει το parent MBR που περιέχει τις εγγραφές του node
   * ώστε να περιέχει μόνο τις εγγραφές που έμειναν στο node μετά την
   * εξαγωγή των toReInsert.
   */
  private def shrinkParent(): Rectangle = {
    val newParent: Rectangle =
      if(node.isLeaf) new Rectangle(node.getEntry(0).asInstanceOf[Point])
                 else node.getEntry(0).asInstanceOf[Rectangle].makeCopy

    val start = if(node.isLeaf) 1 else 0
    for(i <- start until node.getNumberOfEntries)
      newParent.expandRectangle(node.getEntry(i))
    newParent.setChildID(node.getNodeID)
    newParent
  }


}
