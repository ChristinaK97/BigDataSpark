package Geometry

trait GeometricObject {

  def L1: Double

  def distance(p: Point): Double

  def getMemorySize: Int

  def serialize: String

  def getCount: Int


  // only for rectangle
  def setCount(newCount: Int): Unit
  def increaseCount(value: Int): Unit
  def increaseCount(geoObj: GeometricObject): Unit
  def decreaseCount(value: Int): Unit
  def decreaseCount(geoObj: GeometricObject): Unit

  def expandRectangle(geoObj: GeometricObject): Rectangle
}
