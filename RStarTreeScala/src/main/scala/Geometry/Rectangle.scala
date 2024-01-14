package Geometry

import Util.Constants.{N, ONE_MM, sizeOfDouble, sizeOfInt}

class Rectangle extends GeometricObject {

  private var pm: Point = _     //bottom left corner point
  private var pM: Point = _     //upper right corner point
  private var childPtr: Int = _
  private var count: Int = 0

  /** Δημιουγεί rectangle με το pm ως  bottom left corner
   * και το pM ως  upper right corner
   *
   * 2D:   ------- pM
   *      |      |
   *   pm -------
   *
   */
  def this(pm: Point, pM: Point) {
    this()
    this.pm = pm
    this.pM = pM
  }

  def this(pmCoordinates: Array[Double], pMCoordinates: Array[Double]) = {
    this(new Point(pmCoordinates), new Point(pMCoordinates))
  }

  def this(childPtr: Int, count : Int, pmCoordinates: Array[Double], pMCoordinates: Array[Double]) = {
    this(new Point(pmCoordinates), new Point(pMCoordinates))
    this.childPtr = childPtr
    this.count = count
  }

  /** Takes an Int 'N' to create new Points */
  def this(nDims: Int) {
    this(new Point(nDims), new Point(nDims))
  }

  def this(P: Point) {
    this()
    this.pm = new Point(P.getCoordinates.clone())
    this.pM = new Point(P.getCoordinates.clone())
    expandRectangle(P)
  }

  /** @return Επιστρέφει ένα αντίγραφο του αντικειμένου */
  override def makeCopy: Rectangle =
    new Rectangle(this.pm.makeCopy, this.pM.makeCopy)


  def get_pm: Point = pm
  def get_pM: Point = pM


  /** The number of dimensions */
  def nDims: Int =
    pm.nDims

  /** 2 * N dims * sizeof(double) + 2 * sizeof(int) */
  override def getMemorySize: Int = 2 * N * sizeOfDouble + 2 * sizeOfInt

  def getChildID: Int = childPtr

  def setChildID(childID: Int):Unit =
    childPtr = childID


/*---------------------- Rectangle Properties --------------------------------*/

  /** Επιστρέφει το εμβαδόν του rectangle για N=2 διαστάσεις,
   * τον όγκο για N=3   κ.ο.κ.
   */
  def getArea: Double = {
    (0 until N)
      .map(i => Math.abs(pM.getCoordinate(i) - pm.getCoordinate(i)))
      .product
  }


  /** Επιστρέφει την περίμετρο του rectangle. Ορίζεται ως το άθροισμα
   * της διαφοράς  pM[i] - pm[i] για κάθε διάσταση i,
   * δηλαδή κάθε πλευράς, επί το pow(2, Ν-1).
   */
  def getPerimeter: Double = {
    /* υπολογίζει το άθροισμα των Ν διαφορετικών πλευρών
     * N= 2D : μήκος+πλάτος
     * N= 3D : μήκος+πλάτος+ύψος
     *
     * N= 2D : 2 * 2^(2-1) = 4 πλευρο
     * Ν= 3D : 3 * 2^(3-1) = 12 πλευρο
     * Ν= 4D : 4 * 2^(4-1) = 32 πλευρο κ.ο.κ.
    */
    (0 until N)
      .map(i => getSide(i))
      .sum * Math.pow(2, N - 1)
  }

  def getSide(dim: Int): Double =
    Math.abs(pM.getCoordinate(dim) - pm.getCoordinate(dim))


  /** @return Point p: Υπολογίζει και επιστρεφει το κεντρικό σημείο του rectangle */
  def centerPoint: Point = {
    val centerCoor = (0 until N)
      .map(i => (pm.getCoordinate(i) + pM.getCoordinate(i)) / 2)
      .toArray

    new Point(centerCoor)
  }


  def distanceFromCenter(O: GeometricObject): Double = {
    O match {
      case point: Point => distanceFromCenter(point)
      case rectangle: Rectangle => distanceFromCenter(rectangle)
      case _ => 0.0 // Handle other cases or return a default value
    }
  }


  def contains(O: GeometricObject): Boolean = {
    O match {
      case point: Point => contains(point)
      case rectangle: Rectangle => contains(rectangle)
    }
  }


/*----------------------Expand Rectangle--------------------------------*/

  /** Επεκτείνει αυτο το rectangle ώστε το γεωμετρικό αντικείμενο O να γίνει εσσωτερικό του
   * @return true αν έγινε επέκταση του rectangle.
   *         false αν δεν έγινε επέκταση, δηλαδή όταν το P περιέχεται ήδη στο rectangle.
   */
  def expandRectangle(O: GeometricObject): Rectangle = {
    O match {
      case point:     Point     => expandRectangle(point)
      case rectangle: Rectangle => expandRectangle(rectangle)
    }
  }

  /**
   * Επεκτείνει το rectangle ώστε το σημείο P να γίνει εσωτερικό του.
   *
   * - Αν  P[i] > pM[i]           P1.x,y | P2.y| P3.x | P7.y | P8.x
   *       pM[i] <- P[i] + 1mm
   *   Αλλίως το pM[i] αμετάβλητο
   * - Αν P[i] < pm[i]           P4.x | P5.x,y | P6.y | P7.x | P8.y
   *      pm[i] <- P[i] - 1mm
   *   Αλλίως το pm[i] αμετάβλητο
   *
   * @return true αν έγινε επέκταση του rectangle.
   *         false αν δεν έγινε επέκταση, δηλαδή όταν το P περιέχεται
   *         ήδη στο rectangle.
   *
   * --------------------------------------------------------
   * Το pM μετακινείται (επέκταση προς τα πάνω ή και δεξιά):
   *
   *             P2         P1
   *          ----------pM
   *         |         |
   *         |         |    P3
   *         |         |
   *       pm----------
   *
   * --------------------------------------------------------
   *  Το pm μετακινείται (επέκταση προς τα κάτω ή και αριστερά):
   *          ----------pM
   *         |         |
   *    P4   |         |
   *         |         |
   *       pm ----------
   *
   *    P5        P6
   *
   * --------------------------------------------------------
   * Μετακινούνται το  pm (κατά ύψος) και το pM (κατά μήκος)
   *
   *    P7
   *          ----------pM
   *         |         |
   *         |         |
   *         |         |
   *       pm ----------
   *
   *                       P8
   * --------------------------------------------------------
   */
  def expandRectangle(P: Point): Rectangle = {
    for (i <- 0 until N) {

      if (P.getCoordinate(i) >= pM.getCoordinate(i))
        pM.setCoordinate(i, P.getCoordinate(i) + ONE_MM)

      if (P.getCoordinate(i) <= pm.getCoordinate(i))
        pm.setCoordinate(i, P.getCoordinate(i) - ONE_MM)
    }
    this
  }

  /**
   * Επεκτείνει αυτό (this) το rectangle ώστε το Rectange R να βρεθεί εντός
   * του this. Αυτό το πετυχαίνει καλώντας  δύο φορές την expandRectangle(Point),
   * για τα σημεία  R.pm , R.pM. Δηλαδή για να βρεθεί το R εντός του this,
   * αρκεί το this να επεκταθεί ώστε να συμπεριλάβει τα άκρα του R.
   *
   * @return true αν έγινε επέκταση του rectangle.
   *         false αν δεν έγινε επέκταση, δηλαδή όταν το R περιέχεται ήδη στο rectangle.
   */
  def expandRectangle(R: Rectangle): Rectangle = {
    expandRectangle(R.pm)
    expandRectangle(R.pM)
  }


/* ----------------------Σχετικές θέσεις Rectangle και Point ----------------------------*/

  /** Υπολογίζει την ελάχιστη L1 απόσταση του Rectangle από την αρχή των αξόνων ως
   * L1 = Σ_(i in [0,N)) |pm.coordinates(i)|
   */
  override def L1: Double =
    pm.L1


  /** Επιστρέφει true αν το σημείο P βρίσκεται εντός του rectangle
   * Αν το σημείο P βρίσκεται στην περιφέρεια του rectangle θεωρούμε
   * ότι δεν περιέχεται στο rectangle.
   */
  def contains(P: Point): Boolean = {
    for (i <- 0 until N)
      if (pm.getCoordinate(i) >= P.getCoordinate(i) ||      // ---P.i----pm.i----->
          pM.getCoordinate(i) <= P.getCoordinate(i))        // ---pM.i----Pi------>
          return false

    true //για καθε συντεταγμενη i πρεπει να ισχύει  ---pm.i---P.i---pM.i--->
  }


  /** Επιστρέφει την L2 απόσταση του σημείου P από το rectangle
   * Αν το σημείο βρίσκεται μέσα στο rectangle, απόσταση 0.
   */
  override def distance(P: Point): Double = {
    var dist:Double = 0.0
    for (i <- 0 until N) { // για κάθε συντεταγμένη

      if (pm.getCoordinate(i) > P.getCoordinate(i))                       // ---P.i----pm.i----->
          dist += Math.pow(pm.getCoordinate(i) - P.getCoordinate(i), 2)   // (pm.i - P.i)^2

      else if (pM.getCoordinate(i) < P.getCoordinate(i))                  // ---pM.i----Pi----->
          dist += Math.pow(P.getCoordinate(i) - pM.getCoordinate(i), 2)   // (P.i - pM.i)^2
    }
    Math.sqrt(dist)
  }

  /** Υπολογίζει την L2 απόσταση ενός σημείου από το κέντρο του rectangle
   *
   * @param P : Ένα σημείο
   * @return : Την απόσταση του σημείου P από το σημείο κέντρο του rectangle
   */
  def distanceFromCenter(P: Point): Double =
      P.distance(centerPoint)


/* -----------------------Σχετικές θέσεις  δυο  Rectangle------------------------------*/

  /** Υπολογίζει την L2 απόσταση του rectangle R από το κέντρο του (this) rectangle.
   *
   * @param R : Ένα rectangle
   * @return : Η απόσταση απόσταση του σημείου κέντρου του (this) rectangle από
   *         το δοθέν rectangle R.
   */
  def distanceFromCenter(R: Rectangle): Double =
      R.distance(centerPoint)


  /** Επιστρέφει true αν το r βρίσκεται εξ ολοκλήρου  εντός (περιέχει) του rectangle.
   * Αρκεί να ελέγξω αν τα σημεία r.pm και r.pM βρίσκονται εντός του rectangle.
   */
  def contains(r: Rectangle): Boolean =
      contains(r.pm) && contains(r.pM)

  /** Επιστρέφει true αν το rectangle (this) βρίσκεται εξ ολοκλήρου εντός (περιέχεται) του r.
   * Αρκεί να ελέγξω αν τα σημεία pm και pM του rectangle βρίσκονται εντός του r.
   */
  def containedBy(r: Rectangle): Boolean =
      r.contains(pm) && r.contains(pM)


  /** Επιστρέφει true αν το rectangle έχει κοινά σημεία (τέμνεται) με το r.
   */
  def overlaps(r: Rectangle): Boolean = {
    for (i <- 0 until N)
      if (pm.getCoordinate(i) > r.pM.getCoordinate(i) ||  // pM[i] < r.pm[i]
          pM.getCoordinate(i) < r.pm.getCoordinate(i))    // pm[i] > r.pM[i]
          return false
    true
  }


  /** Επιστρέφει το ορθογώνιο (oR) που προκύπτει από την περιοχή overlap
   * αυτού του rectangle (this) και του rectangle R.
   * Αν τα ορθογώνια δεν επικαλύπτονται επιστρέφει null.
   *
   * this
   *     -----------
   *    |           |
   *    |        ---|------
   *    |       |oR |     |
   *    |-------|---|     |
   *            |         |
   *            |---------| R
   *
   * xm  <  R.xm = oR.xm
   *
   * oR.xM = xM <  R.xM
   *
   * Γενικά για την διάσταση i οι συντεταγμένες των oR.pm και oR.pM δίνονται από:
   * pm[i] = max{ this.pm[i], R.pm[i] }
   * pM[i] = min{ this.pM[i], R.pM[i] }
   *
   */
  def rectangleCreatedByOverlap(R: Rectangle): Rectangle = {
    if (!this.overlaps(R))
      return null

    val overlapRect = new Rectangle(N)
    for (i <- 0 until N) {
      overlapRect.pm.setCoordinate(i, Math.max(pm.getCoordinate(i), R.pm.getCoordinate(i)))
      overlapRect.pM.setCoordinate(i, Math.min(pM.getCoordinate(i), R.pM.getCoordinate(i)))
    }
    overlapRect
  }


  /** Επιστρέφει το εμβαδόν του overlap (overlapping area) μεταξύ
   * αυτού του rectangle (this) και του R.
   * Υπολογίζει το overlapRectangle που δημιουργούν τα δύο ορθογώνια
   * μέσω της rectangleCreatedByOverlap
   * και επιστρέφει το εμβαδόν του, που είναι το εμβαδόν της ζητούμενης περιοχής.
   * Αν τα ορθογώνια δεν επικαλύπτονται επιστρέφει 0.
   */
  def overlapArea(R: Rectangle): Double = {
    val overlapRectangle = rectangleCreatedByOverlap(R)
    if (overlapRectangle == null)
        return 0.0 //δεν επικαλύπτονται

    overlapRectangle.getArea
  }


/* ----------------------- Rectangle Serialization  ------------------------------*/

  override def serialize: String = {
    val sb: StringBuilder = new StringBuilder()
    sb.append(childPtr).append(",")
      .append(count).append(",")
      .append(pm.serialize).append(",")
      .append(pM.serialize)
    sb.toString()
  }

  override def toString: String =
    serialize


/* ----------------------- Rectangle Count Aggregation  ------------------------------*/
  override def getCount: Int = count
  override def setCount(newCount: Int): Unit = count = newCount
  override def increaseCount(value: Int): Unit = count += value
  override def increaseCount(geoObj: GeometricObject): Unit = increaseCount(geoObj.getCount)
  override def decreaseCount(value: Int): Unit = count -= value
  override def decreaseCount(geoObj: GeometricObject): Unit = decreaseCount(geoObj.getCount)


  override def dominates(p: Point) : Boolean =
    pm.dominates(p)

  /** Used for calculating the upper limit for the dominance score
   *  of the points contained by this rectangle
   *
   *  pm dominates all points in rectangle r,
   *  but we are not sure if all points in this rectangle
   *  dominate all points in r
   */
  override def dominates(r: Rectangle) : Boolean =
    pm.dominates(r.pm)



}
