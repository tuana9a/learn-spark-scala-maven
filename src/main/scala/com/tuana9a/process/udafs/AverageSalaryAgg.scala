package com.tuana9a.process.udafs

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class TotalSalary(var sum: Double, var count: Long)

object AverageSalaryAgg extends Aggregator[Option[Double], TotalSalary, Double] {
  def zero: TotalSalary = TotalSalary(0L, 0L)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: TotalSalary, salary: Option[Double]): TotalSalary = {
    if (salary.isDefined) {
      buffer.sum += salary.get
      buffer.count += 1
    }
    buffer
  }

  // Merge two intermediate values
  def merge(b1: TotalSalary, b2: TotalSalary): TotalSalary = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // Transform the output of the reduction
  def finish(reduction: TotalSalary): Double = reduction.sum / reduction.count

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[TotalSalary] = Encoders.product

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}