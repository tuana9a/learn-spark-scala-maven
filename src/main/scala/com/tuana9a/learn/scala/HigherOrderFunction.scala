package com.tuana9a.learn.scala

class HigherOrderFunction {
  def test1(f:(Int, Int) => Int): Int = {
    f(1,1)
  }
  def test2(n1: Int, n2: Int): Int = {
    n1 + n2
  }
  def test3(n1: Int): Int => Int = {
    def temp(n2: Int): Int = {
      n1 + n2
    }
    temp
  }
  def test4(): Unit = {
    var addTwo1 = (a:Int, b:Int) => a+b        // Anonymous function by using => (rocket)
    var addTwo2 = (_:Int)+(_:Int)              // Anonymous function by using _ (underscore) wild card
    println(addTwo1(10,10))
    println(addTwo2(10,10))
  }
}
