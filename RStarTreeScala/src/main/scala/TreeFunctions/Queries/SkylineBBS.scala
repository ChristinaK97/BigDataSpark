package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.{LeafNode, TreeNode}
import Util.Constants.DEBUG_SKY
import Util.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SkylineBBS(indexFile: IndexFile, logger: Logger) {

  private val rootID = indexFile.getRootID

  /** Point: Τα σημεία που ανήκουν στην κορυφογραμμή.
   *  Rectangle: Η περιοχή κυριαρχίας του σημείο P είναι ένα ορθωγώνιο με pm= P, pM= [Double.MAX]
   *  Int: nodeID του LeafNode που περιέχει το sky point στα entries του
  */ private val sky : ListBuffer[(Point, Rectangle, Int)] = ListBuffer[(Point, Rectangle, Int)]()

  /** Στοίβα διάσχισης. Κάθε pair (GeometricObject, Double) είναι μία εγγραφή του δέντρου που θα προσπελαστεί
   *  με την ελάχιστη L1 απόσταση από την αρχή των αξόνων (θεωρείται ότι καλύτερα σημεία είναι αυτά με τα MIN
   *  values). Η στοίβα είναι minheap, άρα σημεία κατά την διάσχιση δίνεται προτεραιότητα στα σημεία με τη
   *  μικρότερη απόσταση.
   *  Το Int είναι το nodeID που περιέχει το GeoObj στα entries του
  */ private val heap = mutable.PriorityQueue.empty[(GeometricObject, Int, Double)](
                Ordering.by[(GeometricObject, Int, Double), Double](_._3).reverse)

  logger.info("-"*100 + "\nCompute Skyline\n" + "-"*100)


  /** Υπολογίζει το skyline από τα δεδομένα του δέντρου.
   *  1. Προσθέτει στο minheap τα entries του root node.
   *     Πρέπει να ελεγχθεί το Κ2 (dominance in leaf node) γιατί μπορεί η ρίζα να είναι φύλλο (height==1)
   * 2. Όσο η στοίβα δεν είναι άδεια:
   *    3. Βγάλε την κεφαλή -> geoObj που έχει την ελάχιστη απόσταση από την αρχή των αξόνων σε σχέση με
   *       όλα που έχουν παρατηρηθεί μέχρι τώρα
   *    4. Αν το minEntry είναι:
   *       5. Point: αυτό ανήκει σίγουρα στο skyline
   *       6. Rectangle: Θα πρέπει να το προσπελάσω, δηλαδή να εξετάσω το child node του
   *          Τα entries που child node προστίθονται στο heap, αν ικανοποιούν τα Κ1, Κ2
   */
  def BranchAndBound(): ListBuffer[(Point, Int)] = {
    addToHeap(indexFile.retrieveNode(rootID))                                        //1
    while(heap.nonEmpty) {                                                           //2
      val (minEntry: GeometricObject, nodeID: Int, l1: Double) = heap.dequeue()                   /*3*/                              ;if(DEBUG_SKY) logger.info(s"\nL1 = $l1 \t  ${minEntry.serialize}")

      minEntry match {                                                              //4
        case p: Point     => sky += ((p, p.dominanceArea, nodeID))                          /*5*/                               ;if(DEBUG_SKY) logger.info(s"Add to skyline \t < ${p.serialize} > \t # skyline = ${sky.length}")
        case r: Rectangle => addToHeap(indexFile.retrieveNode(r.getChildID))        //6
      }
    }/*end while not empty*/
    logger.info(toString)
    getSkylinePoints
  }


  private def getSkylinePoints: ListBuffer[(Point, Int)] =
    sky.map { case (p, _, leafNodeID) => (p, leafNodeID) }


  /** Για να εισαχθεί ένα σημείο ως υποψήφιο προς προσπέλαση στην στοίβα
   *  θα πρέπει να μην κυριαρχείται συμφωνα με την συνθήκη isDominant.
   * @param node: Ο κόμβος του δέντρου του οποίου οι εγγραφές (GeoObj)
   *              πρόκειται να εισαχθούν στην στοίβα. Είναι παιδί του
   *              minEntry.
   */
  private def addToHeap(node: TreeNode): Unit = {
    node.zipWithIndex.foreach{case (geoObj, entryIndex) =>
      if(isDominant(entryIndex, geoObj, node)) {
        heap.enqueue((geoObj, node.getNodeID, geoObj.L1))                                                                               ;if(DEBUG_SKY) logger.info(s"\tAdd to heap \t < ${geoObj.L1} : ${geoObj.serialize} > \t # heap = ${heap.length}")
      }
    }
  }


  /** Ένα geoObj δεν κυριαρχείται αν ισχύει ότι:
   *    Κ1) Δεν κυριαρχείται από κάποιο σημείο που έχει εισαχθεί ήδη στο skyline
   *    Κ2) Αν επιπλέον το geoObj isa Point από ένα leaf node, πρέπει να μην κυριαρχείται
   *        από κανένα άλλο σημείο μέσα στο node, δηλ. πρέπει να είναι:
   *          - εξίσου καλό σε όλες τις διαστάσεις και
   *          - καλύτερο σε τουλάχιστον μία.
   *        Πρέπει να ικανοποιεί αυτή τη σχέση με κάθε άλλο σημείο που ανήκει στο node.
   *
   * @param entryIndex: To index του geoObj μέσα στο entriesInNode του node
   * @param geoObj: Ένα Point ή Rectangle που εξετάζω αν πρέπει να μπει στο heap
   * @param node: Ο κόμβος που έχει ως entry αυτό το geoObj
   * @return true αν το geoObj ΔΕΝ κυριαρχείται από κάποιο σημείο του skyline ή, αν είναι και
   *         Point και από κάποιο άλλο point μέσα στο φύλλο που ανήκει
   */
  private def isDominant(entryIndex: Int, geoObj: GeometricObject, node: TreeNode): Boolean = {
    !isDominatedBySky(geoObj) &&
    ( !node.isLeaf || node.asInstanceOf[LeafNode].isDominantInNode(entryIndex) )
  }


  /** Ένα geoObj κυριαρχείται από κάποιο σημείο του skyline αν πέφτει μέσα στην
   * περιοχή κυριαρχίας του.
   *  - Για Point: Βρίσκεται μέσα στο ορθωγώνιο που σχηματίζεται από την περιοχή κυριαρχίας
   *  - Για Rectangle:
   *      - ΌΛΑ τα σημεία που πέφτουν εντός του geoObj κυριαρχούνται από κάποιο skyline point
   *        αν το geoObj περιέχεται εξολοκλήρου μέσα στην περιοχή κυριαρχίας του.
   *        Δεν χρειάζεται να ψάξω για πιθανά skyline points μέσα σε αυτό το geoObj, αφού
   *        όλα τα σημεία του κυριαρχούνται -> pruned
   *      - Αν το geoObj δεν περιέχεται αλλά απλά έχει κάποιο overlap με την περιοχή κυριαρχίας,
   *        μόνο ένα υποσύνολο των σημείων του κυριαρχούνται -> αυτά που βρίσκονται μέσα στο overlap area
   *        Σε αυτή την περίπτωση τα σημεία αυτού του geoObj πρέπει να εξεταστούν για πιθανά skylines
   * @param geoObj: Ένα Point ή Rectangle που εξετάζω αν πρέπει να μπει στο heap
   * @return true αν το geoObj ΔΕΝ κυριαρχείται από κάποιο σημείο του skyline
   */
  private def isDominatedBySky(geoObj: GeometricObject): Boolean = {
    sky.foreach{case (_, skyPointDomArea, _) =>
      if(skyPointDomArea.contains(geoObj))
        return true
    }
    false
  }





//======================================================================================================================
  override def toString: String = {
    val sb: StringBuilder = new StringBuilder()
    sb.append(s"\nSkyline Results :\n\t# points = ${sky.length}\n\t")
    sky.foreach{case (p, domArea, leafNodeID) =>
      sb.append(p.serialize).append(s"\tleafID = $leafNodeID") //.append(s"\t\tDomArea = ${domArea.serialize}")
      sb.append("\n\t")
    }
    sb.toString()
  }
}
