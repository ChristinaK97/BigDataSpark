package Geometry

import Util.Constants.{N, sizeOfDouble}

class Point extends GeometricObject with Serializable {

  private var coordinates: Array[Double] = _ // Declaring coordinates as a mutable empty array

  /** Constructor to initialize coordinates with an array of length N */
  def this(nDims: Int) {
    this()
    coordinates = Array.ofDim[Double](nDims)
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
  def nDims: Int =
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


  def dominanceArea: Rectangle = {
    new Rectangle(
      this.coordinates,
      Array.fill[Double](N)(Double.MaxValue)
    )
  }

  /**
   * In order for p to dominate q, the following conditions need to be met:
   *
   * 1) For all dimensions: p must be better than or equal to q
   * 2) In at least one dimension: p must be strictly better than q
   *
   * (better = minimum)
   *
   * If both conditions are satisfied, p dominates q.
   * If any dimension fails the first condition or
   * all dimensions satisfy the first condition but none satisfy the second condition,
   * then p does not dominate q.
   **/
  def dominates(q: Point): Boolean = {
    var foundStrictlyBetter = false
    for(i <- 0 until N) {
      if(this.coordinates(i) < q.coordinates(i))
        foundStrictlyBetter = true
      else if(this.coordinates(i) > q.coordinates(i))
        return false
    }
    foundStrictlyBetter
  }


  /** Υπολογίζει την L1 απόσταση του σημείου από την αρχή των αξόνων ως
   * L1 = Σ_(i in [0,N)) |coordinates(i)|
   */
  override def L1: Double =
    coordinates.map(math.abs).sum


  /** Υπολογίζει την L2 απόσταση μεταξύ δύο σημείων.
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

  override def toString: String =
    serialize

}

