package com.tuana9a.learn.scala

import scala.util.control.Breaks.{break, breakable}

class Break {
  def test1(): Unit = {
    breakable { // Breakable method to avoid exception
      for (i <- 1 to 10 by 2) {
        if (i == 7)
          break() // com.tuana9a.Break used here
        else
          println(i)
      }
      println("reach1")
    }
  }

  def test2(): Unit = {
    //EXPLAIN: break con me nó đến tận block, chứ không phải for ạ :))
    breakable { // Breakable method to avoid exception
      for (i <- 1 to 10 by 2) {
        if (i == 7)
          break() // com.tuana9a.Break used here
        else
          println(i)
      }
      println("reach1")
    }
    println("reach2")
  }
}
