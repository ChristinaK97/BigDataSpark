package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{GeometricObject, Point, Rectangle}
import TreeStructure.{LeafNode, TreeNode}
import Util.Constants.DEBUG_TOPK
import Util.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TopK(indexFile: IndexFile, k: Int, logger: Logger) {

  var kVar: Int = setK()                                                                                                ; if(DEBUG_TOPK) logger.info(s"New k = $kVar")
  val root: TreeNode = indexFile.retrieveNode(indexFile.getRootID)

  /** Σωρός διάσχισης. Κορυφή είναι το rectangle με το upper bound dom score, δηλ
   *  εκείνο του οποίου τα σημεία είναι πιο πιθανό να ανήκουν στο αποτέλεσμα
   */ val travelMaxHeap: mutable.PriorityQueue[Rectangle] = mutable.PriorityQueue.empty[Rectangle](
                         Ordering.by(rectangle => rectangle.getDomScore))

  /** Σωρός με τα top-k σημεία. Κρατάει k σημεία κάθε φορά και κορυφή είναι το σημείο με
   *  το k-th dom score, δηλ το ελάχιστο/χειρότερο σκορ μεταξύ των top-k
   */ implicit val topKOrdering: Ordering[Point] = Ordering.by(point => - point.getDomScore)
      val topKMinHeap: mutable.PriorityQueue[Point] = mutable.PriorityQueue.empty[Point](topKOrdering)

  /** Το dom score της τρέχουσας κεφαλής (το ελάχιστο/χειρότερο σκορ μεταξύ των top-k
  */  var kthScore: Int = 0

// ---------------------------------------------------------------------------------------------------------------------

  /** Εκτελεί τον αλγόριθμο SCG (Mamoulis) για τον υπολογισμό των top k σημείων στο δοθέν aR*tree
   *  @return MinHeap με αυτά τα σημεία. Κεφαλή είναι το σημείο με το k-th dom score.
   */
  def SimpleCountGuidedAlgorithm(): mutable.PriorityQueue[Point] = {
    /* 1. Για την ρίζα του δέντρου τρέχει το batch count
     *    - Αν η ρίζα είναι ο μοναδικός κόμβος και φύλλο του δέντρου που περιέχει όλα τα σημεία
     *      του dataset, έχει υπολογιστεί το dom score για όλα αυτά τα σημεία με all-to-all έλεγχο
     *      Εισάγονται απευθείας στο minheap και κρατάμε μόνο τα k καλύτερα
     *
     * 2. Αλλίως ο αλγ τρέχει κανονικά όπως περιγράφεται στο paper.
     *    3. Όλα τα rectangle entries του root είσαγονται στο maxheap διάσχυσης
     *    4. Όσο ο σωρός δεν είναι άδειος και η κορυφή του έχει καλύτερο score από το
     *       τρέχον k-th σημείο στο top-k:
     *       5. Εξάγεται από το travel το rectangle με το μέγιστο upper bound dom score
     *          : είναι το πιο promising για να βρούμε σημεία που θα ανήκουν στο αποτέλεσμα
     *       6. Κατεβαίνουμε στις εγγραφές του child node του maxMBR και υπολογίζεται το
     *          - dom score των points, αν childNode isa Leaf
     *          - upper bound dom score των points που περικλύονται από τα rectangle entries στο childNode
     *       7. Αν το childNode isa Leaf με Points,  ενημερώνεται ο σωρός topK με βάση το dom score τους
     *       8. Αλλιώς αν isa NonLead με Rectangles, όλα εισάγωνται στο σωρό travel με βάση το upper bound τους
     */
    BatchCount(root, root)
    if(root.isLeaf)                                                                /*1*/
      updateTopKMinHeap(root.asInstanceOf[LeafNode])

    else {                                                                         /*2*/
      updateTravelMaxHeap(root)                                                    /*3*/

      while (continueSearch()) {                                            /*4*/
        val maxMBR: Rectangle   = travelMaxHeap.dequeue()                         /*5*/
        val childNode: TreeNode = indexFile.retrieveNode(maxMBR.getChildID)       /*6*/                                 ; if(DEBUG_TOPK) logger.info(s"\n# travelMaxHeap = ${travelMaxHeap.size+1}\n\tPop ${maxMBR.toString}\tpm.domScore = ${maxMBR.getDomScore}\tcount = ${maxMBR.getCount}\t childID = [${maxMBR.getChildID}]\n\tchildNode id = [${childNode.getNodeID}]\tisLeaf? ${childNode.isLeaf}\t # entries = ${childNode.getNumberOfEntries}\t [${childNode.getNodeID}].SCount = ${childNode.getSCount}")
        BatchCount(root, childNode)

        if (childNode.isLeaf)                                                     /*7*/
          updateTopKMinHeap(childNode.asInstanceOf[LeafNode])
        else                                                                     /*8*/
          updateTravelMaxHeap(childNode)                                                                                ; if(DEBUG_TOPK) logger.info(s"\tAdd # entries = ${childNode.getNumberOfEntries} to travelMaxHeap")
      }
    }//end root isa NonLeaf
    logger.info(toString)                                                                                               ; if (DEBUG_TOPK) logger.info(s"travelMaxHeap.size = ${travelMaxHeap.size}\t travel.head = ${if (travelMaxHeap.nonEmpty) travelMaxHeap.head.getDomScore else "empty"} \t k-th score = ${topKMinHeap.head.getDomScore}")
    topKMinHeap
  }

  def setK(): Int =
    k.min(indexFile.getNumOfPoints)


  private def continueSearch(): Boolean = {
    travelMaxHeap.nonEmpty && (topKMinHeap.size < kVar || travelMaxHeap.head.getDomScore >= kthScore)
  }


  def updateTravelMaxHeap(node: TreeNode): Unit = {
    travelMaxHeap.addAll(node.getEntries.asInstanceOf[ListBuffer[Rectangle]])
  }


  /** Ενημερώνει το MinHeap των Top K σημείων με τα σημεία του leaf
   *  για τα οποία έχει υπολογιστεί το dom score μέσω του batch count.
   *  Το heap κρατάει κάθε φορά τα k σημεία με τα μέγιστα dom scores
   *  και κορυφή είναι το σημείο με το ελάχιστο (k-th) dom score,
   *  δηλ το χειρότερο σκορ στο heap μέχρι τώρα.
   *  1. Όσο ο σωρός χωράει σημεία, πρόσθεσε τα χωρίς έλεγχο
   *  2. Αν βρέθηκαν k σημεία, το p θα μπει στον σωρό μόνο αν έχει
   *     μεγαλύτερο dom score από το σημείο κεφαλή (σε αυτή την
   *     περίπτωση η κεφαλή - kth - βγαίνει και εισάγεται το p)
   *  3. Ενημερώνεται το kthScore με το dom score της τρέχουσας κεφαλής
   */
  def updateTopKMinHeap (node: LeafNode): Unit = {                                                                      ;  if(DEBUG_TOPK) logger.info(s"\tUpdate topK with [${node.getNodeID}]\t # entries = ${node.getNumberOfEntries}")
    node.foreach{ case p: Point =>
      addPointToTopKMinHeap(p)
    }
    kthScore = topKMinHeap.head.getDomScore /*3*/
  }


  def addPointToTopKMinHeap(p:Point): Unit = {
    if(topKMinHeap.size < kVar) {                                    /*1*/
      topKMinHeap.enqueue(p)                                                                                            ;  if(DEBUG_TOPK) logger.info(s"\t\ttopK.size=${topKMinHeap.size-1} < k \t topK.add(${p.toString} ,\t${p.getDomScore})")
    } else if( topKOrdering.compare(p, topKMinHeap.head) < 0 ) {  /*2*/                                                 ;  if(DEBUG_TOPK) logger.info(s"\t\ttopK full\t [${p.getPointID}].domScore=${p.getDomScore} > [${topKMinHeap.head.getPointID}].domScore=${topKMinHeap.head.getDomScore}\t topK.add(${p.toString} ,\t${p.getDomScore})")
      topKMinHeap.dequeue()
      topKMinHeap.enqueue(p)
    }
  }

// BATCH COUNT ---------------------------------------------------------------------------------------------------------

  /** Εκτελεί τον αλγόριθμο Batch Count για το batch.
   *
   * @param node: Ένας κόμβος του δέντρου που τα count aggr values των node entries θα
   *              χρησιμοποιηθούν για να υπολογιστεί το dom score των αντικειμένων του batch
   * @param batch: Ο κόμβος του δέντρου που αν είναι:
   *  - LeafNode με Points, υπολογίζεται το dom score κάθε Point
   *  - NonLeaf  με Rectangles, υπολογίζεται το upper limit του dom score όλων των
   *    Points που περικλύονται από το κάθε rectangle entry.
   */
  private def BatchCount(node: TreeNode, batch: TreeNode) : Unit = {                                                    //  logger.info(s"\t\t${batch.map(entry => if(entry.isInstanceOf[Point]) s"[${entry.asInstanceOf[Point].getPointID}].PdomScore=${entry.getDomScore}" else s"[${entry.asInstanceOf[Rectangle].getChildID}].RdomScore=${entry.getDomScore}" )}\n\t\tnode = [${node.getNodeID}] isLeaf? ${node.isLeaf}")
    /* 1. Για κάθε nodeEntry στο node  // το node αρχικά είναι η ρίζα του δέντρου
     *    2. Αν το node isa NonLeaf με Rectangles ΚΑΙ υπάρχει κάποιο αντικείμενο στο batch *
     *       που κυριαρχεί μόνο μερικώς στο Rectangle nodeEntry:
     *       3. Αφού μόνο partial dominance πρέπει να κατέβω στις εγγραφές του child node
     *
     *       * if batch isa Leaf    => batchEntry isa Point,     υπολογίζεται το dom score του
     *         if batch isa NonLeaf => batchEntry isa Rectangle, χρησιμοποιείται το R.pm για να υπολογιστεί
     *                                                           upper bound dom score των Points του MBR
     *
     *    4. Αλλιώς, αν node isa Leaf OR δεν βρέθηκε σημείο στο batch που να κυριαρχεί μερικώς το nodeEntry
     *       5. Για κάθε σημείο στο batch που κυριαρχεί στο node entry
     *          6. Αύξησε το dom score του σημείου κατά:
     *             - 1, αν το nodeEntry isa Point που κυριαρχείται από το batchEntry
     *               (όταν !node.isLeaf είναι false)
     *             - nodeEntry.count, αν το node isa Rectangle, του οποίου όλα τα σημεία κυριαρχούνται από το batchEntry
     *               Συνεπώς προσθέτω στο dom score του batchEntry το πλήθος αυτών των κυριαρχούμενων σημείων
     *               (όταν !node.isLeaf είναι true αλλά το hasPartialDom είναι false)
     */
    node.foreach{ nodeEntry =>                                                       /*1*/                              //  logger.info(s"\t\t\tnode entry = ${nodeEntry.toString}")
      if(!node.isLeaf && hasPartialDom(nodeEntry.asInstanceOf[Rectangle], batch)) { /*2*/
        val childID = nodeEntry.asInstanceOf[Rectangle].getChildID                                                      //; logger.info(s"\t\t\t\tnode [${node.getNodeID}] is NonLeaf and nodeEntry is partially dominated by some point in batch. GOTO child [${childID}]")
        BatchCount(indexFile.retrieveNode(childID), batch)                          /*3*/
      }else                                                                        /*4*/                                                                                                     //  logger.info(s"\t\t\t\tnode [${node.getNodeID}] is leaf or node entry is not partially dominated")
        increaseDomScores(batch, nodeEntry)
    }//end foreach nodeEntry
  }


  def increaseDomScores(batch: TreeNode, nodeEntry: GeometricObject): Unit = {
    batch.foreach { batchEntry =>
      if (batchEntry.dominates(nodeEntry))              /*5*/
        batchEntry.increaseDomScore(nodeEntry.getCount) /*6*/
    }
  }



  /** Εξετάζει αν το node entry (ένα rectangle από το NonLeaf) κυριαρχείται
   *  μερικώς από κάποιο σημείο στο batch. Δηλ υπάρχει κάποιο σημείο στο batch
   *  το οποίο κυριαρχεί στο r.pM αλλά όχι στο pm
   *
   *         |
   *      ---|----------------  pM
   *     |   |   ε P DOM     |
   *     |   |   REGION      |
   *  pm ----|---------------
   *         |
   *         p-------------------
   *
   */
  def hasPartialDom(r: Rectangle, batch: TreeNode): Boolean = {
    batch.foreach{ batchEntry =>
      if( !batchEntry.dominates(r.get_pm) &&
           batchEntry.dominates(r.get_pM))
        return true
    }
    false
  }






//======================================================================================================================
  override def toString: String = {
    val sb = new StringBuilder()
    val topKCopy = topKMinHeap.clone()
    while(topKCopy.nonEmpty) {
      val p = topKCopy.dequeue()
      sb.append(s"\t${p.toString}\tDom Score = ${p.getDomScore}\n")
    }
    sb.toString()
  }


}



