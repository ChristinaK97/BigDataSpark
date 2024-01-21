

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


    println("=================Skyline Calculation=================")
    // Read from the file and create an RDD of Points.
    val pointsRDD: RDD[Point] = sc.textFile(filePath)
                                  .zipWithIndex()        // Pair each line with its index
                                  .filter(_._2 > 0)      // Skip the first line (index 0)
                                  .map(_._1)             // Keep only the line content
                                  .map(line => Point(line.split(",").map(_.toDouble).toList))

    // Compute Skyline
    val skyline: RDD[Point] = computeSkyline(pointsRDD,spark)

    // Collect and print the Skyline
    skyline.collect().foreach(point => println(point.dimensions.mkString(",")))

    println("=================Top-K Calculation=================")

    // Broadcast the points as a list to each node
    val pointsList = spark.sparkContext.broadcast(pointsRDD.collect().toList)

    // Compute dominance scores for each point as a new RDD of tuples (Point, Score)
    val dominanceScoresRDD: RDD[(Point, Int)] = pointsRDD.map { point =>
      val score = pointsList.value.count(otherPoint => dominates(point, otherPoint))
      (point, score)
    }

    // Get the value of k from args or use a default value
    val k = if (args.length > 0) args(0).toInt else 10 // For example, default is 10

    // Get top k points with the highest dominance scores
    val topKPoints: Array[(Point, Int)] = dominanceScoresRDD
      .sortBy(_._2, ascending = false)
      .take(k)

    // Print the top k points with their dominance scores
    topKPoints.foreach { case (point, score) =>
      println(s"Point: ${point.dimensions.mkString(",")}, Score: $score")
    }

    println("=================Skyline Top-K Calculation=================")

    // Compute dominance scores only for the skyline points
    val skylineDominanceScoresRDD: RDD[(Point, Int)] = skyline.map { skypoint =>
      val score = pointsList.value.count(otherPoint => dominates(skypoint, otherPoint))
      (skypoint, score)
    }

    // Get top k skyline points with the highest dominance scores
    val topKSkylinePoints: Array[(Point, Int)] = skylineDominanceScoresRDD
      .sortBy(_._2, ascending = false)
      .take(k)

    // Print the top k skyline points with their dominance scores
    topKSkylinePoints.foreach { case (point, score) =>
      println(s"Skyline Point: ${point.dimensions.mkString(",")}, Score: $score")
    }

    // Stop the Spark session
    spark.stop()
    }
}
