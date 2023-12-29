package Geometry

trait GeometricObject {

  def L1: Double

  def distance(p: Point): Double

  def getMemorySize: Int

  def serialize: String
}
