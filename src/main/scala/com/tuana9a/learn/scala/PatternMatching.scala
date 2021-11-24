package com.tuana9a.learn.scala

class PatternMatching {
  def test1(): Unit = {
    search(11)
    search(1)
    search("Two")
  }
  def search (a:Any):Any = a match{
    case 1  => println("One")
    case "Two" => println("Two")
    case "Hello" => println("Hello")
    case _ => println("No")
  }
}
