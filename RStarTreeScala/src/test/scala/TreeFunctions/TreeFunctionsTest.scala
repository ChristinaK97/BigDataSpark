package TreeFunctions

import Geometry.Point

import java.io.{BufferedReader, FileReader}

object TreeFunctionsTest {

  def readPointsFromCSV(csvFilePath: String): Iterable[Point] = {
    val points = scala.collection.mutable.ListBuffer[Point]()

    val fileReader = new FileReader(csvFilePath)
    val bufferedReader = new BufferedReader(fileReader)

    var line: String = null
    var isFirstLine = true
    var headers: Array[String] = Array.empty[String]
    var pointID: Int = 0
    while ( {line = bufferedReader.readLine(); line != null}) {
      if (isFirstLine) {
        headers = line.split(",").map(_.trim)
        isFirstLine = false
      } else {
        val coordinates:Array[Double] = line.split(",").map(_.trim)
          .map(_.toDouble)
        points += new Point(pointID, coordinates)
        pointID += 1
      }
    }
    bufferedReader.close()
    points
  }


  def main(args: Array[String]): Unit = {
    val dataPath = "C:/Users/karal/progr/Scala/BigDataSpark/dist_generator/uniform_large.csv"
    val points: Iterable[Point] = readPointsFromCSV(dataPath)
    val nDims = points.head.nDims
    val start = System.nanoTime()
    val rTree = new RStarTree(points.iterator, nDims)
    rTree.createTree("0")
    val end = System.nanoTime()
    val totalTreeCreationTime = (end-start).toDouble  / 1e9
    println(s"\nTotal aR*Trees creation time =\t $totalTreeCreationTime")
    val(_,_,_, times) = rTree.runQueries(10, 10)
    times.foreach{case (start, end) =>
      println((end-start).toDouble / 1e9)
    }
    rTree.close()
  }
}
