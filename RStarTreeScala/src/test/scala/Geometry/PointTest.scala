package Geometry

import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper
import org.scalatest.funsuite.AnyFunSuite

class PointTest extends AnyFunSuite {

  test("Test Point constructor 1: Point dimensionality") {
    val N: Int = 2
    assert(new Point(N).N == N)
  }

  test("Test Point constructor 2: Point from array") {
    val coordinates: Array[Double] = Array(1, 2, 3, 4, 5)
    val p: Point = new Point(coordinates)
    assert(p.N == coordinates.length)
    assert(p.getCoordinate(1) == coordinates(1))

    p.setCoordinate(0, 10.0)
    assert(p.getCoordinate(0) == 10.0)
  }

  test("Test distance of two points: 1D") {
    assert(
      new Point(Array(0.0)).distance(new Point(Array(3.0))) == 3.0)
    assert(
      new Point(Array(-1.0)).distance(new Point(Array(3.0))) == 4.0)
  }

  test("Test distance of two points: higher-D") {
    assert(
      new Point(Array(1.2, 3.0)).distance(new Point(Array(2.5, 4.7))) === Math.sqrt(4.58) +- 1e-9)
    assert(
      new Point(Array(0.5, -3.2, 7, -2.7)).distance(new Point(Array(2.2, -6.1, -0.2, 3.1))) === Math.sqrt(96.78) +- 1e-9)
  }

}
