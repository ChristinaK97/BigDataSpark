package Geometry

import Util.Constants.N
import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper
import org.scalatest.funsuite.AnyFunSuite

class RectangleTest extends AnyFunSuite {

  test("Test Rectangle constructor 1: Rectangle dimensionality") {
    N = 2
    assert(new Rectangle(N).nDims == N)
  }
  test("Test Rectangle constructor 2: Rectangle from Points") {
    N = 2
    assert(
      new Rectangle(new Point(N), new Point(N)).nDims == N)
    assert(
      new Rectangle(new Point(Array(2.2,7,3)), new Point(Array(2.3,7,2))).get_pm.getCoordinate(0) == 2.2)
  }
  test("Test 2-D Rectangle area") {
    N = 2
    val r: Rectangle = new Rectangle(new Point(Array(2.2, 3.0)), new Point(Array(3.1, 0.2)))
    assert(r.getArea === 2.52 +- 1e-9)

    r.get_pM.setCoordinate(1, -1.3)
    r.get_pm.setCoordinate(0, -0.2)
    assert(r.get_pm.getCoordinate(0) == -0.2)
    assert(r.getArea === 14.19 +- 1e-9)
  }
  test("Test N-D Rectangle hyper-volume") {
    N = 3
    assert(
      new Rectangle(new Point(Array(2.2, 3.0, -0.6)), new Point(Array(3.1, 0.2, 1.8))).getArea === 6.048 +- 1e-9)
    assert(
      new Rectangle(new Point(Array(-2.2, -3.0, -0.6)), new Point(Array(3.1, 0.2, -1.8))).getArea === 20.352 +- 1e-9)
    N = 4
    assert(
      new Rectangle(new Point(Array(2.2, 3.0, -0.6, -0.7)),
                    new Point(Array(3.1, 0.2, 1.8, 1.48))).getArea === 13.18464 +- 1e-9)
    N = 5
    assert(
      new Rectangle(new Point(Array(-2.2, 3.0, -0.6, -0.7, 2.7)),
                    new Point(Array(3.1, 0.2, -1.8, 1.48, 4.2))).getArea === 58.23216 +- 1e-9)
  }

}
