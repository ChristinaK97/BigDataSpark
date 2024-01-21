
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

// Define a case class representing a point in two dimensions.
case class Point(dimensions: List[Double])

object SimpleSkylineCalculation {

  // Utility function to determine if point A dominates point B.
  def dominates(a: Point, b: Point): Boolean = {
    val notWorseInAny = a.dimensions.zip(b.dimensions).forall {
      case (aDim, bDim) => aDim <= bDim // A is not worse than B in any dimension
    }
    val betterInAtLeastOne = a.dimensions.zip(b.dimensions).exists {
      case (aDim, bDim) => aDim < bDim // A is strictly better than B in at least one dimension
    }
    notWorseInAny && betterInAtLeastOne
  }

  def computeSkyline(pointsRDD: RDD[Point], spark: SparkSession): RDD[Point] = {
    import spark.implicits._

    // Broadcast the points as a list to each node
    val pointsList = spark.sparkContext.broadcast(pointsRDD.collect().toList)

    // Filter the RDD by checking if a point is dominated by any point in the broadcast variable
    pointsRDD.filter { point =>
      !pointsList.value.exists(otherPoint => dominates(otherPoint, point) && otherPoint != point)
    }.distinct()
  }

  def main(args: Array[String]): Unit = {
    // Setup Spark
    val spark = SparkSession.builder().appName("SkylineComputation").master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val filePath = "/home/marios/projects/ApacheScalaTesting/input/generated_data.csv"  // Replace with the actual file path.

    // Read from the file and create an RDD of Points.
    val pointsRDD: RDD[Point] = sc.textFile(filePath)
      .map(line => Point(line.split(",").map(_.toDouble).toList))

    // Compute Skyline
    val skyline: RDD[Point] = computeSkyline(pointsRDD,spark)

    // Collect and print the Skyline
    skyline.collect().foreach(point => println(point.dimensions.mkString(",")))

    // Stop the Spark session
    spark.stop()
    }
}