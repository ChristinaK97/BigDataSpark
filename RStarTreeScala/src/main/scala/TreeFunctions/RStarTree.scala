package TreeFunctions

import FileHandler.IndexFile
import Geometry.Point
import Util.Constants.N
import Util.Logger

class RStarTree(pointsPartition: Iterator[Point], nDims: Int) {
  N = nDims
  private var indexfile: IndexFile = _

  def createTree(rTreeID: Long) : Unit = {
    indexfile = new IndexFile(rTreeID)
    new CreateTree(indexfile, pointsPartition, new Logger())
    indexfile.closeFile()
  }
}
