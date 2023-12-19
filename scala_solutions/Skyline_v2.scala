// # With Docstrings

import org.apache.spark.sql.{DataFrame, SparkSession}
// import org.apache.spark.sql.functions.{col, concat_ws, udf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.Encoders


case class Point(dimensions: Seq[Double])

object SkylineDominance {

  def isDominated(p: Point, q: Point): Boolean = {
    // Defines a function isDominated that takes two points (p and q) and checks
    // if p dominates q. The function uses zip to pair up the dimensions of the two points and
    // checks the dominance conditions.
    p.dimensions.zip(q.dimensions).forall { case (pi, qi) => pi <= qi } &&
    p.dimensions.zip(q.dimensions).exists { case (pi, qi) => pi < qi }
  }

  implicit val pointEncoder: org.apache.spark.sql.Encoder[Point] = Encoders.product[Point]

  def skylineSet(points: DataFrame): DataFrame = {
    val spark = points.sparkSession

    import spark.implicits._

    val pointDataset = points.as[Point]
    pointDataset
      .filter(p =>
        pointDataset.filter(q => p != q && isDominated(q, p)).count() == 0
      )
      .toDF()
  }

  /** Main entry point for the SkylineDominance application. Reads data from a
    * CSV file, computes the skyline, dominance score, and shows the skyline
    * points and top k points with the highest dominance score.
    *
    * @param args
    *   Command line arguments (not used in this example).
    */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SkylineDominance")
      .master(
        "local"
      ) // Change to "yarn" or other cluster manager when running on a cluster.
      .getOrCreate()

    // Reading the data from CSV file
    val filePath = "/home/marios/projects/Big_Data_Skyline_Project/BigDataSpark/dist_generator/generated_data.csv"
    val inputData: DataFrame = spark.read.option("header", "true").csv(inputFile)
      .select(inputColumns: _*)
      .as[Point](pointEncoder)  // Provide the explicit Encoder for Point
      .toDF()

    // Skyline computation using dominance definition
    val skylineDF = skylineSet(inputData)

    // show the skyline points
    skylineDF.show()

    spark.stop()
  }

// Function to dynamically extract column names
  def inputColumns: Seq[Column] = {
    val numDimensions = getNumDimensions(
      inputFile,
      spark
    ) // Pass inputFile and SparkSession to getNumDimensions
    (0 until numDimensions).map(i => col(s"Dim_$i").cast("double"))
  }

  // Function to get the number of dimensions dynamically by counting commas in the first row
  def getNumDimensions(inputFile: String, spark: SparkSession): Int = {
    val firstRow =
      spark.read.option("header", "true").csv(inputFile).limit(1).collect()(0)
    val numDimensions = firstRow.length
    numDimensions
  }
}
