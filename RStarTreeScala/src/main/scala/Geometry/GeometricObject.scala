package Geometry

trait GeometricObject {

  def distance(p: Point): Double

  def getMemorySize: Int

  def serialize: String
}
