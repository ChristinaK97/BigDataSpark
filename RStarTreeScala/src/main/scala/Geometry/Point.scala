package Geometry

import Util.Constants.sizeOfDouble

class Point extends GeometricObject {

  private var coordinates: Array[Double] = _ // Declaring coordinates as a mutable empty array

  /** Constructor to initialize coordinates with an array of length N */
  def this(N: Int) {
    this()
    coordinates = Array.ofDim[Double](N)
  }

  /** Constructor to set the coordinates field */
  def this(coordinates: Array[Double]) {
    this()
    this.coordinates = coordinates
  }

  /** @return Επιστρέφει ένα αντίγραφο του αντικειμένου */
  def makeCopy: Point =
    new Point(coordinates.clone())


  /** The number of dimensions */
  def N: Int =
    coordinates.length

  /** N dims * sizeof(double) */
  override def getMemorySize: Int = N * sizeOfDouble

  def getCoordinates: Array[Double] =
    coordinates

  /** Επιστρέφει την τιμή της συντεταγμένης dim του σημείου */
  def getCoordinate(dim: Int): Double =
    coordinates(dim)

  /** Θέτει την τιμή της συντεταγμένης c του P σε value
   * P[c] <- value */
  def setCoordinate(dim: Int, value: Double): Unit =
    coordinates(dim) = value


  /** Υπολογίζει την απόσταση μεταξύ δύο σημείων.
   *
   * @param P : Ένα άλλο σημείο
   * @return : Την απόσταση αυτό (this) του σημείου από το δοθέν σημείο P
   */
  override def distance(P: Point): Double = {
    val dist = (0 until N)
      .map(i => Math.pow(this.getCoordinate(i) - P.getCoordinate(i), 2))
      .sum
    Math.sqrt(dist)
  }

  override def serialize: String = {
    val sb = new StringBuilder()
    coordinates.zipWithIndex.foreach { case (coordinate, dim) =>
      sb.append(coordinate)
      if(dim < N-1)
        sb.append(",")
    }
    sb.toString()
  }

}

