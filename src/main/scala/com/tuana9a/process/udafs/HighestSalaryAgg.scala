package com.tuana9a.process.udafs

import com.tuana9a.model.data.UserData
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class HighestSalary(var id: Int, var salary: Double)

object HighestSalaryAgg extends Aggregator[UserData, HighestSalary, String] {
  override def zero: HighestSalary = HighestSalary(-1, -1)

  override def reduce(b: HighestSalary, a: UserData): HighestSalary = {
    if (a.salary > b.salary) {
      b.id = a.id
      b.salary = a.salary
    }
    b
  }

  override def merge(b1: HighestSalary, b2: HighestSalary): HighestSalary = {
    if (b1.salary > b2.salary) b1 else b2
  }

  override def finish(reduction: HighestSalary): String = {
    reduction.id.toString + "###" + reduction.salary.toString
  }

  override def bufferEncoder: Encoder[HighestSalary] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}