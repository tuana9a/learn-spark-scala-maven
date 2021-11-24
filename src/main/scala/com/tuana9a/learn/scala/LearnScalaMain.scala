package com.tuana9a.learn.scala

object LearnScalaMain {
  def main(args: Array[String]): Unit = {
    //    test1()
    //    test2()
    //    test3()
    //    test4()
    //    test5()
    //    test6()
    //    test7()
    //    test8()
    //    test9()
    test10()
  }

  def test1(): Unit = {
    new JavaCode().test1()
    val ages = Seq(42, 75, 29, 64)
    println(s"The oldest person is ${ages.max}")
    var temp: Unit = new Variables().test1()
    println(temp)
  }

  def test2(): Unit = {
    var conditions = new Conditions()
    conditions.test1()
    conditions.test2()
    conditions.test3()
  }

  def test3(): Unit = {
    var patternMatching = new PatternMatching
    patternMatching.test1()
  }

  def test4(): Unit = {
    var loop = new Loop
    //    loop.test1()
    //    loop.test2()
    //    loop.test3()
    //    loop.test4()
    //    loop.test5()
    //    loop.test6()
    //    loop.test7()
    //    loop.test8()
    loop.test9()
  }

  def test5(): Unit = {
    var breaks = new Break()
    breaks.test1()
    breaks.test2()
  }

  def test6(): Unit = {
    var functions = new Functions()
    functions.test1()
    println(functions.test2())
    functions.test3(1, 2)
    println(functions.test4(2, 3))
    println(functions.test5())
    println(functions.test4(a = 2, b = 3))
  }

  def test7(): Unit = {
    var higherOrderFunction = new HigherOrderFunction()
    println(higherOrderFunction.test1(higherOrderFunction.test2))
    var addTwo = higherOrderFunction.test3(2)
    println(addTwo(10))
    higherOrderFunction.test4()
  }

  def test8(): Unit = {
    val fun1 = (a: Int, b: Int) => a + b // Anonymous function by using => (rocket)
    println(fun1(10, 10))
    val fun2 = (_: Int) + (_: Int) // Anonymous function by using _ (underscore) wild card
    println(fun2(10, 10))
    val fun3 = (_: Double) * (_: Int) + (_: String)
    println(fun3(1, 2, "abc"))
  }

  def test9(): Unit = {
    var functions = new Functions()
    functions.test6()
    println(functions.test7())
    println(functions.test7(1, 2, 3))
    //    var f: Functions = null
    //    f.test6()
  }

  def test10(): Unit = {
    var tuples = new Tuples()
    tuples.test1()
  }
}
