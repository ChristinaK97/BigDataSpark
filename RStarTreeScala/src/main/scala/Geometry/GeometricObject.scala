package Geometry

abstract class GeometricObject {

  def L1: Double

  def distance(p: Point): Double

  def getMemorySize: Int

  def serialize: String

  def getCount: Int

  def makeCopy: GeometricObject


  def dominates(geoObj: GeometricObject): Boolean = {
    geoObj match {
      case q: Point => dominates(q)
      case r: Rectangle => dominates(r)
    }
  }
  def dominates(p: Point): Boolean
  def dominates(r: Rectangle): Boolean

  // dom scores for top k ---------------------------------------------
  private var domScore: Int = _

  def getDomScore: Int = domScore
  def resetDomScore(): Unit = domScore = 0
  def increaseDomScore(value: Int): Unit = domScore += value
  // ------------------------------------------------------------------


  // only for rectangle
  def setCount(newCount: Int): Unit
  def increaseCount(value: Int): Unit
  def increaseCount(geoObj: GeometricObject): Unit
  def decreaseCount(value: Int): Unit
  def decreaseCount(geoObj: GeometricObject): Unit

  def expandRectangle(geoObj: GeometricObject): Rectangle
}
