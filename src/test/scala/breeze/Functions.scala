package breeze

import breeze.numerics.sigmoid
import org.junit.Test

class Functions {
  @Test
  def testSigmoid: Unit = {
    println(s"sigmoid(3)  = ${sigmoid(3)}")
    println(s"sigmoid(1)  = ${sigmoid(1)}")
    println(s"sigmoid(0)  = ${sigmoid(0)}")
    println(s"sigmoid(-1) = ${sigmoid(-1)}")
    println(s"sigmoid(-3) = ${sigmoid(-3)}")

    println(s"sigmoid(1) + sigmoid(-1) = ${sigmoid(1) + sigmoid(-1)}")
    println(s"sigmoid(3) + sigmoid(-3) = ${sigmoid(3) + sigmoid(-3)}")
  }
}
