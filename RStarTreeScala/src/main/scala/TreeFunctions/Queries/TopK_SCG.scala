package TreeFunctions.Queries

import FileHandler.IndexFile
import Geometry.{Point, Rectangle}
import TreeStructure.{LeafNode, TreeNode}
import Util.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TopK_SCG(k: Int, indexFile: IndexFile, logger: Logger) {

  private val root = indexFile.retrieveNode(indexFile.getRootID)

  private val travelMaxHeap = mutable.PriorityQueue.empty[Rectangle](
              Ordering.by(rectangle => rectangle.getDomScore))

  implicit val topKOrdering: Ordering[Point] = Ordering.by(point => - point.getDomScore)
  private  val topKMinHeap = mutable.PriorityQueue.empty[Point](topKOrdering)

  private var kthScore: Int = 0

  // -------------------------------------------------------------------------------------------------------------------

  def SimpleCountGuidedAlgorithm(): mutable.PriorityQueue[Point] = {
    BatchCount(root)
    if(root.isLeaf)
      updateTopKMinHead(root.asInstanceOf[LeafNode])

    else {
      travelMaxHeap.addAll(root.getEntries.asInstanceOf[ListBuffer[Rectangle]])

      while (travelMaxHeap.nonEmpty &&
             travelMaxHeap.head.getDomScore >= kthScore) {
        val maxMBR: Rectangle   = travelMaxHeap.dequeue()
        val childNode: TreeNode = indexFile.retrieveNode(maxMBR.getChildID)                                             ; logger.info(s"\n# travelMaxHeap = ${travelMaxHeap.size+1}\n\tPop ${maxMBR.toString}\tpm.domScore = ${maxMBR.getDomScore}\tcount = ${maxMBR.getCount}\t childID = [${maxMBR.getChildID}]\n\tchildNode id = [${childNode.getNodeID}]\tisLeaf? ${childNode.isLeaf}\t # entries = ${childNode.getNumberOfEntries}\t [${childNode.getNodeID}].SCount = ${childNode.getSCount}")
        BatchCount(childNode)

        if (childNode.isLeaf)
          updateTopKMinHead(childNode.asInstanceOf[LeafNode])
        else
          travelMaxHeap.addAll(childNode.getEntries.asInstanceOf[ListBuffer[Rectangle]])                                ; logger.info(s"\tAdd # entries = ${childNode.getNumberOfEntries} to travelMaxHeap")
      }
    }//end root isa NonLeaf
    logger.info(toString)
    topKMinHeap
  }


  private def updateTopKMinHead(node: LeafNode): Unit = {                                                                 logger.info(s"\tUpdate topK with [${node.getNodeID}]\t # entries = ${node.getNumberOfEntries}")
    node.foreach{ case p: Point =>
      if(topKMinHeap.size < k) {
        topKMinHeap.enqueue(p)                                                                                          ; logger.info(s"\t\ttopK.size=${topKMinHeap.size-1} < k \t topK.add(${p.toString} ,\t${p.getDomScore})")
      } else if( topKOrdering.compare(p, topKMinHeap.head) < 0 ) {                                                        logger.info(s"\t\ttopK full\t [${p.getPointID}].domScore=${p.getDomScore} > [${topKMinHeap.head.getPointID}].domScore=${topKMinHeap.head.getDomScore}\t topK.add(${p.toString} ,\t${p.getDomScore})")
        topKMinHeap.dequeue()
        topKMinHeap.enqueue(p)
      }
    }
    kthScore = topKMinHeap.head.getDomScore
  }


  private def BatchCount(batch: TreeNode): Unit = {
    batch.foreach{ batchEntry => batchEntry.resetDomScore() }                                                           ; logger.info(s"\tCalculate BatchCount for [${batch.getNodeID}]\tisRoot? ${batch.getNodeID==root.getNodeID}\tisLeaf? ${batch.isLeaf}\t# entries = ${batch.getNumberOfEntries}\t[${batch.getNodeID}].SCount = ${batch.getSCount}")
    BatchCount(root, batch)
  }


  private def BatchCount(node: TreeNode, batch: TreeNode) : Unit = {                                                    //  logger.info(s"\t\t${batch.map(entry => if(entry.isInstanceOf[Point]) s"[${entry.asInstanceOf[Point].getPointID}].PdomScore=${entry.getDomScore}" else s"[${entry.asInstanceOf[Rectangle].getChildID}].RdomScore=${entry.getDomScore}" )}\n\t\tnode = [${node.getNodeID}] isLeaf? ${node.isLeaf}")
    node.foreach{ nodeEntry =>                                                                                          //  logger.info(s"\t\t\tnode entry = ${nodeEntry.toString}")
      if(!node.isLeaf && hasPartialDom(nodeEntry.asInstanceOf[Rectangle], batch)) {
        val childID = nodeEntry.asInstanceOf[Rectangle].getChildID                                                      //; logger.info(s"\t\t\t\tnode [${node.getNodeID}] is NonLeaf and nodeEntry is partially dominated by some point in batch. GOTO child [${childID}]")
        BatchCount(indexFile.retrieveNode(childID), batch)
      }
      else{                                                                                                             //  logger.info(s"\t\t\t\tnode [${node.getNodeID}] is leaf or node entry is not partially dominated")
        batch.foreach{ batchEntry =>
          if(batchEntry.dominates(nodeEntry))
            batchEntry.increaseDomScore(nodeEntry.getCount)
        }
      }//end else
    }//end foreach nodeEntry
  }


  private def hasPartialDom(r: Rectangle, batch: TreeNode): Boolean = {
    batch.foreach{ batchEntry =>
      if( !batchEntry.dominates(r.get_pm) &&
           batchEntry.dominates(r.get_pM))
        return true
    }
    false
  }

// ---------------------------------------------------------------------------------------------------------------------
  override def toString: String = {
    val sb = new StringBuilder()
    val topKCopy = topKMinHeap.clone()
    sb.append(s"\nTOP K = $k RESULTS :\n")
    while(topKCopy.nonEmpty) {
      val p = topKCopy.dequeue()
      sb.append(s"\t${p.toString}\tDom Score = ${p.getDomScore}\n")
    }
    sb.toString()
  }


}


/*private def batchCount(node: TreeNode, pointsBatch: List[Point]) : Unit = {
  node.foreach{geoObj =>

    if(node.isInstanceOf[NonLeafNode] && hasPartialDom(geoObj.asInstanceOf[Rectangle], pointsBatch)) {
      val childID = geoObj.asInstanceOf[Rectangle].getChildID
      batchCount(indexFile.retrieveNode(childID), pointsBatch)
    }
    else
      pointsBatch.foreach(p =>
        if(p.dominates(geoObj))
          p.increaseDomScore(geoObj.getCount)
      )
  }//end foreach entry
}

private def hasPartialDom(r: Rectangle, pointsBatch: List[Point]): Boolean = {
  pointsBatch.foreach{ p =>
    if(!p.dominates(r.get_pm) && p.dominates(r.get_pM))
      return true
  }
  false
}*/
