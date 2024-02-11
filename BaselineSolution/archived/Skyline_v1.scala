/* SimpleApp.scala */
// import org.apache.spark.sql.SparkSession

// object SkylineApp {
//   def main(args: Array[String]): Unit = {
//     val logFile = "/opt/spark/README.md" // Should be some file on your system
//     val spark = SparkSession.builder.appName("Skyline").getOrCreate()
//     val logData = spark.read.textFile(logFile).cache()
//     val numAs = logData.filter(line => line.contains("a")).count()
//     val numBs = logData.filter(line => line.contains("b")).count()
//     println(s"Lines with a: $numAs, Lines with b: $numBs")
//     spark.stop()
//   }
// }


// import org.apache.spark.sql.{DataFrame, SparkSession}
// import org.apache.spark.sql.functions.{col, not, udf}
// import org.apache.spark.sql.expressions.UserDefinedFunction

// object SkylineDominance {

//   def main(args: Array[String]): Unit = {
//     val spark: SparkSession = SparkSession.builder()
//       .appName("SkylineDominance")
//       .master("local") // Change to "yarn" or other cluster manager when running on a cluster.
//       .getOrCreate()

//     // Reading the data from CSV file
//     val filePath = "path_to_your_file.csv"
//     val data = spark.read.option("header", value = true).csv(filePath)

//     // Assuming that all dimensions are numeric
//     val dimCols = data.columns.filter(_.startsWith("Dim_")).map(col)

//     // Skyline computation using dominance definition
//     val skylineDF = computeSkyline(spark, data, dimCols)

//     // show the skyline points
//     skylineDF.show()

//     // Dominance score computation and show top k dominated points
//     val k = 5 // Arbitrary value for k points
//     val dominanceDF = computeDominanceScore(spark, data, skylineDF, dimCols)

//     // show the k points with the highest dominance score
//     dominanceDF.orderBy(col("dominanceScore").desc).limit(k).show()

//     // Show the k skyline points with the highest dominance score
//     val skylineDominanceDF = skylineDF.join(dominanceDF, skylineDF.columns.head, "inner")
//     skylineDominanceDF.orderBy(col("dominanceScore").desc).limit(k).show()

//     spark.stop()
//   }

//   def computeSkyline(spark: SparkSession, data: DataFrame, dimCols: Array[org.apache.spark.sql.Column]): DataFrame = {
//     import spark.implicits._

//     val dominanceUDF: UserDefinedFunction = udf((x: Seq[Double], y: Seq[Double]) => x.zip(y).forall { case (xi, yi) => xi >= yi })

//     data.cache()
//     val points = data.select(dimCols: _*).as[Seq[Double]]

//     val skyline = points.rdd.flatMap(x => {
//       points.collect { case y if dominanceUDF(x, y) && x != y => (x, 1) }
//     }).reduceByKey(_ + _).filter(_._2 == points.count() - 1).keys.distinct

//     spark.createDataFrame(skyline.map(Row => Row), data.schema)
//   }

//   def computeDominanceScore(spark: SparkSession, data: DataFrame, skylineDF: DataFrame, dimCols: Array[org.apache.spark.sql.Column]): DataFrame = {
//     import spark.implicits._

//     val dominanceUDF: UserDefinedFunction = udf((x: Seq[Double], y: Seq[Double]) => x.zip(y).forall { case (xi, yi) => xi >= yi })

//     data.cache()
//     val points = data.select(dimCols: _*).as[Seq[Double]]

//     val dominanceScore = points.rdd.map(x => {
//       val score = points.map(y => if (dominanceUDF(y, x) && x != y) 1 else 0).reduce(_ + _)
//       (x, score)
//     }).toDF("point", "dominanceScore")

//     val dominanceScoreDF = dominanceScore.withColumn("point", col("point").cast("string"))
//     val explodedDF = data.withColumn("point", concat_ws(",", dimCols: _*))

//     explodedDF.join(dominanceScoreDF, "point")
//   }

//   // Util function to convert a Row of any numerical type to Seq[Double]
//   private def rowToDoubleSeq(row: org.apache.spark.sql.Row): Seq[Double] = {
//     row.toSeq.map {
//       case value: Int => value.toDouble
//       case value: Long => value.toDouble
//       case value: Float => value.toDouble
//       case value: Double => value
//       case _ => throw new IllegalArgumentException("Non-numeric value encountered")
//     }.asInstanceOf[Seq[Double]]
//   }
// }
// ```

// To clear up, the `computeSkyline` and `computeDominanceScore` functions are making transformations on Spark RDDs instead of DataFrames, mainly because they use custom UDFs and iterations over data points which are more intuitive to express with RDD transformations. Spark UDFs, however, are generally less efficient than DataFrame/Dataset operations due to their black-box nature that prevents Spark's catalyst optimizer from optimizing them. In practice, for large datasets, you would want to leverage DataFrame operations as much as possible.

// Before running this code, ensure that you have the right Spark session setup and that your CSV file path is correct. When running on a Spark cluster, replace `.master("local")` with the appropriate cluster manager details.

// Lastly, the Skyline problem is known to be challenging when it comes to massive datasets with higher dimensions. This implementation is an elementary approach, and for more efficient and scalable solutions, you might want to research techniques like space partitioning, block-nested loops, or specialized distributed skyline algorithms.



// # With Docstrings

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, udf}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.Row

object SkylineDominance {

  /**
    * Main entry point for the SkylineDominance application.
    * Reads data from a CSV file, computes the skyline, dominance score, and
    * shows the skyline points and top k points with the highest dominance score.
    *
    * @param args Command line arguments (not used in this example).
    */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SkylineDominance")
      .master("local") // Change to "yarn" or other cluster manager when running on a cluster.
      .getOrCreate()

    // Reading the data from CSV file
    val filePath = "path_to_your_file.csv"
    val data = spark.read.option("header", value = true).csv(filePath)

    // Assuming that all dimensions are numeric
    val dimCols = data.columns.filter(_.startsWith("Dim_")).map(col)

    // Skyline computation using dominance definition
    val skylineDF = computeSkyline(spark, data, dimCols)

    // show the skyline points
    skylineDF.show()

    // Dominance score computation and show top k dominated points
    val k = 5 // Arbitrary value for k points
    val dominanceDF = computeDominanceScore(spark, data, skylineDF, dimCols)

    // show the k points with the highest dominance score
    dominanceDF.orderBy(col("dominanceScore").desc).limit(k).show()

    // Show the k skyline points with the highest dominance score
    val skylineDominanceDF = skylineDF.join(dominanceDF, skylineDF.columns.head, "inner")
    skylineDominanceDF.orderBy(col("dominanceScore").desc).limit(k).show()

    spark.stop()
  }

  /**
    * Computes the skyline points from the input data based on the dominance definition.
    *
    * @param spark   SparkSession
    * @param data    Input DataFrame
    * @param dimCols Array of numeric columns representing dimensions
    * @return DataFrame containing the skyline points
    */
  def computeSkyline(spark: SparkSession, data: DataFrame, dimCols: Array[org.apache.spark.sql.Column]): DataFrame = {
    import spark.implicits._

    // User-defined function for dominance check
    val dominanceUDF: UserDefinedFunction = udf((x: Seq[Double], y: Seq[Double]) => x.zip(y).forall { case (xi, yi) => xi >= yi })

    data.cache()
    val points = data.select(dimCols: _*).as[Seq[Double]]

    // Compute skyline points using dominance definition
    val skyline = points.rdd.flatMap(x => {
      points.collect { case y if dominanceUDF(x, y) && x != y => (x, 1) }
    }).reduceByKey(_ + _).filter(_._2 == points.count() - 1).keys.distinct

    // Create a DataFrame from the computed skyline points
    spark.createDataFrame(skyline.map(Row => Row), data.schema)
  }

  /**
    * Computes the dominance score for each point in the input data based on the skyline points.
    *
    * @param spark      SparkSession
    * @param data       Input DataFrame
    * @param skylineDF  DataFrame containing the skyline points
    * @param dimCols    Array of numeric columns representing dimensions
    * @return DataFrame containing dominance scores for each point
    */
  def computeDominanceScore(spark: SparkSession, data: DataFrame, skylineDF: DataFrame, dimCols: Array[org.apache.spark.sql.Column]): DataFrame = {
    import spark.implicits._

    // User-defined function for dominance check
    val dominanceUDF: UserDefinedFunction = udf((x: Seq[Double], y: Seq[Double]) => x.zip(y).forall { case (xi, yi) => xi >= yi })

    data.cache()
    val points = data.select(dimCols: _*).as[Seq[Double]]

    // Compute dominance score for each point
    val dominanceScore = points.rdd.map(x => {
      val score = points.map(y => if (dominanceUDF(y, x) && x != y) 1 else 0).reduce(_ + _)
      (x, score)
    }).toDF("point", "dominanceScore")

    // Convert point column to string type for joining
    val dominanceScoreDF = dominanceScore.withColumn("point", col("point").cast("string"))

    // Create a DataFrame with exploded points for joining
    val explodedDF = data.withColumn("point", concat_ws(",", dimCols: _*))

    // Join the exploded DataFrame with dominance scores
    explodedDF.join(dominanceScoreDF, "point")
  }

  /**
    * Util function to convert a Row of any numerical type to Seq[Double].
    *
    * @param row Input Row
    * @return Sequence of Double values
    */
  private def rowToDoubleSeq(row: org.apache.spark.sql.Row): Seq[Double] = {
    row.toSeq.map {
      case value: Int => value.toDouble
      case value: Long => value.toDouble
      case value: Float => value.toDouble
      case value: Double => value
      case _ => throw new IllegalArgumentException("Non-numeric value encountered")
    }.asInstanceOf[Seq[Double]]
  }
}