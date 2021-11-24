package com.tuana9a.learn.scala

class Loop {
  def test1(): Unit = {
    var a = 10
    while (a <= 20) { // Condition
      println(a)
      a = a + 2 // Incrementation
    }
  }

  def test2(): Unit = {
    var a = 10; // Initialization
    do {
      println(a)
      a = a + 2; // Increment
    }
    while (a <= 20) // Condition
  }

  def test3() {
    for (a <- 1 to 10) {
      println(a)
    }
  }

  def test4(): Unit = {
    for (a <- 1 until 10) {
      println(a)
    }
  }

  def test5(): Unit = {
    for (a <- 1 to 10 if a % 2 == 0) {
      println(a);
    }
  }

  def test6(): Unit = {
    var list = List(1, 2, 3, 4, 5, 6, 7, 8, 9) // Creating a list
    for (i <- list) { // Iterating the list
      println(i)
    }
  }

  def test7(): Unit = {
    var list = List(1, 2, 3, 4, 5, 6, 7, 8, 9) // Creating a list
    for (i <- list if i > 5) { // Iterating the list
      println(i)
    }
  }

  def test8(): Unit = {
    var list = List(1, 2, 3, 4, 5, 6, 7, 8, 9) // Creating a list
    list.foreach {
      println // Print each element
    }
    list.foreach(print)
    println
    list.foreach((element: Int) => print(element + " ")) // Explicitly mentioning type of elements
  }

  def test9(): Unit = {
    for (i <- 1 to 10 by 2) {
      println(i)
    }
    println
    for (i <- 1 to 10 by 3) {
      println(i)
    }
  }
}
