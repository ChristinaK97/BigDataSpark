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

    while ( {line = bufferedReader.readLine(); line != null}) {
      if (isFirstLine) {
        headers = line.split(",").map(_.trim)
        isFirstLine = false
      } else {
        val coordinates:Array[Double] = line.split(",").map(_.trim)
          .map(_.toDouble)
        points += new Point(coordinates)
      }
    }
    bufferedReader.close()
    points
  }


  def main(args: Array[String]): Unit = {
    val dataPath = "C:/Users/karal/progr/Scala/BigDataSpark/dist_generator/uniform.csv"
    val points: Iterable[Point] = readPointsFromCSV(dataPath)
    val nDims = points.head.nDims
    new RStarTree(points.iterator, nDims).createTree(0)
  }
}
