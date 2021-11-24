package com.tuana9a.learn.scala

class Functions {
  def test1() { // Defining a function
    println("This is a simple function")
  }

  def test2(): Int = {
    var r = 1
    r
  }

  def test3(n1: Int, n2: Int): Int = {
    println(n1 + n2)
    n1 + n2
  }

  def test4(a: Int, b: Int): Int = {
    if (b == 0) // Base condition
      0
    else
      a + test4(a, b - 1)
  }

  def test5(a: Int = 1, b: Int = 2): Int = {
    a + b
  }

  def add(a: Int)(b: Int) = {
    a + b
  }

  def test6(): Unit = {
    var result1 = add(10)(10)
    println("add(10)(10) = " + result1)

    var addIt = add(10) _ //EXPLAIN: equal "var test = add(10) (_)"
    println("add(10) = Int => Int" + result1)
    var result2 = addIt(3)
    println("10 + 3 = " + result2)


    var result3 = addIt(3)
    println("10 + 3 = " + result3)
  }

  def test7(args: Int*): Int = {
    var sum = 0;
    for (a <- args) sum += a
    sum
  }
}
