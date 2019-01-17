object TripleEqual {
  case class Table(value: Int) {
    var that: Table = this

    override def equals(obj: scala.Any): Boolean = {
      println(s"equals $obj")
      super.equals(obj)
    }

    override def hashCode(): Int = {
      println(s"hashCode on $value")
      super.hashCode()
    }

    def ==(tbl: Table): Boolean = {
      println(s"== $value")
      value == tbl.value
    }

    def ===(tbl: Table): Table = {
      this.that = tbl
      this
    }

    def eval(): Boolean = {
      println(s"eval $value == ${that.value}")
      this.value == that.value
    }


  }

  def output(a: Boolean): Unit = {
    println(a)
  }

  def output2(tbl: Table): Unit = {
    println(tbl)

    println(tbl.eval())


  }
  def main(args: Array[String]): Unit = {
    val a = Table(1)

    val b = Table(2)

    output(a == b)

    output2(a === b)

  }

}
