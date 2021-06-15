package com.tuana9a.udafs

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

case class UserDataIdSalary(var id: Int, var salary: Double)

object HighestSalaryAgg extends Aggregator[UserDataIdSalary, UserDataIdSalary, String] {
  override def zero: UserDataIdSalary = UserDataIdSalary(-1, -1)

  override def reduce(b: UserDataIdSalary, a: UserDataIdSalary): UserDataIdSalary = {
    if (a.salary > b.salary) {
      b.id = a.id
      b.salary = a.salary
    }
    b
  }

  override def merge(b1: UserDataIdSalary, b2: UserDataIdSalary): UserDataIdSalary = {
    if (b1.salary > b2.salary) b1 else b2
  }

  override def finish(reduction: UserDataIdSalary): String = {
    reduction.id.toString + "###" + reduction.salary.toString
  }

  override def bufferEncoder: Encoder[UserDataIdSalary] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}