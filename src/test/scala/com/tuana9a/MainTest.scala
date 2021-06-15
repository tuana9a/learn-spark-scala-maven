package com.tuana9a

import org.junit._

import scala.collection.mutable.ListBuffer

@Test
class MainTest {
  val EXAMPLES_DIR = "/mnt/sda1/RESOURCE/txt/"

  def genSingleLine800mb(): Unit = {
    val path = EXAMPLES_DIR + "single-line-800mb.txt"
    Examples.genSingleLine(path, 3, 200000000)
  }

  def genMultiLine800mb(): Unit = {
    val path = EXAMPLES_DIR + "multi-line-800mb.txt"
    Examples.genMultiLine(path, 3, 200000000, 20)
  }

  @Test
  def test0(): Unit = {
    println("hello world!")
  }

  @Test
  def test1(): Unit = {
    def fun(s: String*): Unit = {
      s.foreach(println)
    }

    val temp: ListBuffer[String] = new ListBuffer[String]
    temp += "tuan"
    temp += "tuan1"
    temp += "tuan2"
    fun(temp: _*)
  }

  @Test
  def test2(): Unit = {
    val list1 = new ListBuffer[String]
    list1 += "10"
    list1 += "9"
    list1 += "8"
    val list2 = Seq("10", "9", "8")
    assert(list1.contains("10"))
    assert(list2.contains("10"))
    assert(list1.contains("8"))
    assert(!list1.contains("5"))
  }
}


