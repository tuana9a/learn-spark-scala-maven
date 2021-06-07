package com.tuana9a

import com.tuana9a.process.WordCount
import com.tuana9a.tests.{TestAdmicro, TestAny, TestParquet}

import scala.collection.mutable.ListBuffer

object Main {
  private var tests = new ListBuffer[(String, Any => Unit)]()
  def add(n: String, f: Any => Unit): Unit = {
    tests = tests += Tuple2(n, f)
  }
  Main.add("wc", WordCount.run)
  Main.add("admicro", TestAdmicro.run)
  Main.add("parquet", TestParquet.run)
  Main.add("any", TestAny.run)

  def main(args: Array[String]): Unit = {
    var arg0 = "any"
    var arg1 = "0"
    try {
      arg0 = args(0)
      arg1 = args(1)
    } catch {
      case e: Exception =>
        println(e + ": Bad Args: " + args.mkString("args(", ", ", ")"))
        return
    }
    tests.foreach(e => {
      if (e._1.equals(arg0)) {
        e._2(arg1)
      }
    })
  }
}
