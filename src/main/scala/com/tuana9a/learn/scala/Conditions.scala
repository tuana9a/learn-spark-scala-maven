package com.tuana9a.learn.scala

class Conditions {
  def test1(): Unit = {
    val age: Int = 20;
    if (age > 18) {
      println("Age is greater than 18")
    }
  }
  def test2(): Unit = {
    val age: Int = 20;
    if (age > 18) {
      println("Age is greater than 18")
    } else {
      println("Age is lower than 18")
    }
  }

  def test3() {
    val result = checkIt(-10)
    println (result)
  }

  private def checkIt (a:Int): Int =  if (a >= 0) 1 else -1    // Passing a if expression value to function
}
