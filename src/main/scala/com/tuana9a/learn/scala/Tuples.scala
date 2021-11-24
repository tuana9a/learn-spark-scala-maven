package com.tuana9a.learn.scala

class Tuples {
  def test1(): Unit = {
    var tuples = (1, 2, "tuan", 4)
    println(tuples)
    println(tuples._1)
    println(tuples._2)
    println(tuples._3)
    for (elem <- tuples.productIterator) {
      println(elem)
    }
    tuples.productIterator.foreach(println)
  }
}
