package com.tuana9a.udafs

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

object HighestSalaryAgg extends Aggregator[IdSalary, IdSalary, String] {
  override def zero: IdSalary = IdSalary(-1, -1)

  override def reduce(b: IdSalary, a: IdSalary): IdSalary = {
    if (a.salary > b.salary) {
      b.id = a.id
      b.salary = a.salary
    }
    b
  }

  override def merge(b1: IdSalary, b2: IdSalary): IdSalary = {
    if (b1.salary > b2.salary) b1 else b2
  }

  override def finish(reduction: IdSalary): String = {
    reduction.id.toString + "###" + reduction.salary.toString
  }

  override def bufferEncoder: Encoder[IdSalary] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}