package com.tuana9a.udafs

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable.ListBuffer


object TestAggs {
  case class CaseClass1(var x1: Long, var x2: Long)

  object Aggregator1 extends Aggregator[CaseClass1, CaseClass1, CaseClass1] {
    def zero: CaseClass1 = CaseClass1(0L, 0L)

    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(b: CaseClass1, a: CaseClass1): CaseClass1 = {
      b.x1 += a.x1
      b.x2 += a.x2
      b
    }

    // Merge two intermediate values
    def merge(b1: CaseClass1, b2: CaseClass1): CaseClass1 = {
      b1.x1 += b2.x1
      b1.x2 += b2.x2
      b1
    }

    // Transform the output of the reduction
    def finish(reduction: CaseClass1): CaseClass1 = reduction

    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[CaseClass1] = Encoders.product

    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[CaseClass1] = Encoders.product
  }

  case class CaseClass2(var id: Long, var x1: Long, var x2: Long)

  object Aggregator2 extends Aggregator[CaseClass2, ListBuffer[CaseClass2], ListBuffer[CaseClass2]] {
    def zero: ListBuffer[CaseClass2] = new ListBuffer[CaseClass2]()

    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(b: ListBuffer[CaseClass2], a: CaseClass2): ListBuffer[CaseClass2] = {
      b += a
      b
    }

    // Merge two intermediate values
    def merge(b1: ListBuffer[CaseClass2], b2: ListBuffer[CaseClass2]): ListBuffer[CaseClass2] = {
      b1 ++ b2
    }

    // Transform the output of the reduction
    def finish(reduction: ListBuffer[CaseClass2]): ListBuffer[CaseClass2] = {
      reduction
    }

    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[ListBuffer[CaseClass2]] = ExpressionEncoder()

    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[ListBuffer[CaseClass2]] = ExpressionEncoder()
  }

  object GroupListCaseClass2 extends Aggregator[CaseClass2, ListBuffer[CaseClass2], ListBuffer[CaseClass2]] {
    // init buffer
    def zero: ListBuffer[CaseClass2] = new ListBuffer[CaseClass2]

    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: ListBuffer[CaseClass2], data: CaseClass2): ListBuffer[CaseClass2] = {
      buffer += data
      buffer
    }

    // Merge two intermediate values
    def merge(b1: ListBuffer[CaseClass2], b2: ListBuffer[CaseClass2]): ListBuffer[CaseClass2] = {
      b1 ++ b2
    }

    // CaseClass2ransform the output of the reduction
    def finish(reduction: ListBuffer[CaseClass2]): ListBuffer[CaseClass2] = {
      reduction
    }

    override def bufferEncoder: Encoder[ListBuffer[CaseClass2]] = ExpressionEncoder()

    // Specifies the Encoder for the final output value type
    override def outputEncoder: Encoder[ListBuffer[CaseClass2]] = ExpressionEncoder()
  }

  case class CaseClass3(id: Int, list: ListBuffer[CaseClass2])

  case class CaseClass4(var id: Long, caseCLass2: CaseClass2)

  object Aggregator3 extends Aggregator[CaseClass2, ListBuffer[CaseClass2], CaseClass2] {
    def zero: ListBuffer[CaseClass2] = new ListBuffer[CaseClass2]()

    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(b: ListBuffer[CaseClass2], a: CaseClass2): ListBuffer[CaseClass2] = {
      b += a
      b
    }

    // Merge two intermediate values
    def merge(b1: ListBuffer[CaseClass2], b2: ListBuffer[CaseClass2]): ListBuffer[CaseClass2] = {
      b1 ++ b2
    }

    // Transform the output of the reduction
    def finish(reduction: ListBuffer[CaseClass2]): CaseClass2 = {
      reduction.reduce((x1, x2) => {
        x1.x1 += x2.x1
        x1.x2 += x2.x2
        x1
      })
    }

    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[ListBuffer[CaseClass2]] = ExpressionEncoder()

    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[CaseClass2] = Encoders.product
  }
}
