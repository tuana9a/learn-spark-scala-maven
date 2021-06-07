package com.tuana9a

import org.junit._

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
  def helloWorld(): Unit = {
    println("hello world!")
  }

}


