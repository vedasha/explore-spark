package breeze

import breeze.linalg.{DenseMatrix, DenseVector}
import org.junit.Test

class BasicBreeze {
  @Test
  def testVector: Unit = {
    // Create a zero vector with 3 values
    val v3 = DenseVector.zeros[Double](3)
    println(v3)

    // transpose the vector
    println(v3.t)

    val v = DenseVector(0, 1, 2, 3)
    println(v)

    println(s"The first value is ${v(0)}")
    println(s"The  last value is ${v(-1)}, negative index is supported like in python")
  }

  @Test
  def testZeros: Unit = {
    // Create a 3 * 2 zero matrix
    // And the value type is Double
    val zeroDouble = DenseMatrix.zeros[Double](3,2)
    println(zeroDouble)

    // We could also create zero with value type Int
    val zeroInt = DenseMatrix.zeros[Int](3,2)
    println(zeroInt)


  }


  def main(args: Array[String]): Unit = {
    val m3x2 = DenseMatrix.rand(3, 2)
    val m2x1 = DenseMatrix.rand(2, 1)
    println(m3x2)
    val m3x1 = m3x2 * m2x1
    println(m3x1)

    val z3x1 = DenseMatrix.zeros[Int](3, 1)

    println(z3x1)
    println(z3x1.t)
  }
}
