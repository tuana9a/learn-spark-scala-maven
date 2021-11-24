package com.tuana9a.learn.spark

import scala.collection.mutable.ListBuffer

object Main {
  case class RunTest(name: String, run: Any => Unit)

  private var tests = new ListBuffer[RunTest]()

  def add(n: String, f: Any => Unit): Unit = {
    tests = tests += RunTest(n, f)
  }

  add("TestAny", TestAny.run)
  add("TestUdfs", TestUdfs.run)
  add("TestUdafs", TestUdafs.run)
  add("TestParquet", TestParquet.run)

  def main(args: Array[String]): Unit = {
    val arg0 = args(0)
    val arg1 = args(1)
    tests.foreach(e => {
      if (e.name.equals(arg0)) {
        e.run(arg1)
      }
    })
  }
}
