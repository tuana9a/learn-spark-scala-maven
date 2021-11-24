package com.tuana9a.learn.scala

class Variables {
  def test1(): Unit = {
    var num = 100
    num = 101
    val immutableNum = 100
    println(immutableNum)
    //    immutableNum = 102//EXPLAIN: const so can't not reassign
  }
}
