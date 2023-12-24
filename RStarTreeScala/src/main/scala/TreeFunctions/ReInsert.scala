package TreeFunctions

import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.TreeNode

import scala.collection.mutable

/**
 * @param treeNode : To TreeNode όπου προέκυψε overflow
 */
class ReInsert(treeNode: TreeNode) {

  private val node: TreeNode = treeNode
  private val toReInsert: mutable.Stack[(GeometricObject, Int)] = mutable.Stack[(GeometricObject, Int)]()


  /** 1. Επιστρέφει μια λίστα με τα στοιχεία (εγγραφές) που πρέπει να αφαιρεθούν
   * από το node και να γίνουν reinsert στο δέντρο. (λίστα toReInsert)
   *
   * @param MBR  : Το Rectangle MBR που περιέχει τις εγγραφές του node
   * @return ListBuffer[(GeometricObject, Int)] .
   *         Το πεδίο TreeNode είναι
   *             - Rectangle αν node instanceof NonLeafNode
   *             - Point αν node instanceof LeafNode
   */
  def makeToReInsert(MBR: Rectangle, reInsertLevel: Int): mutable.Stack[(GeometricObject, Int)] = {

    val entrySDistance =
      node.getEntries.zipWithIndex.map { case (entry, index) =>
            ( index, MBR.distanceFromCenter(entry) )
      }
      .sortBy(_._2).reverse  //sort by distance from center, desc

    //το 30% του κόμβου θα επανεισαχθεί
    val splitIndex = (node.getNumberOfEntries * 0.3).toInt

    entrySDistance.take(splitIndex).foreach{case (index, _) =>
      toReInsert += ((node.getEntry(index), reInsertLevel))
    }
    toReInsert
  }


  /** 2. Να καλείται μετά την make_toReInsert
   * Διαγράφει από το entriesInNode του node τις εγγραφές που πρέπει
   * να επαναεισαχθούν στο δέντρο.
   */
  def updateNode(): Unit = {
    toReInsert.foreach{case (entry:GeometricObject, _) =>
      node.deleteEntry(entry)
    }
  }


  /** 3. Συρρικνώνει το parent MBR που περιέχει τις εγγραφές του node
   * ώστε να περιέχει μόνο τις εγγραφές που έμειναν στο node μετά την
   * εξαγωγή των toReInsert.
   */
  def shrinkParent(): Rectangle = {
    val newParent =
      if(node.isLeaf) new Rectangle(node.getEntry(0).asInstanceOf[Point])
                 else node.getEntry(0).asInstanceOf[Rectangle].makeCopy

    val start = if(node.isLeaf) 1 else 0
    for(i <- start until node.getNumberOfEntries)
      newParent.expandRectangle(node.getEntry(i))
    newParent
  }


}
