package FileHandler

import TreeStructure.TreeNode

class IndexFile {
  private var IOs: Int = 0

  def updateMetadata(rootID: Int, treeHeight: Int) : Unit = {

  }

  def writeNodeToFile(node: TreeNode): Unit = {
    IOs += 1
  }

  def retrieveNode(nodeID: Int): TreeNode = {
    IOs += 1
    null
  }

}
