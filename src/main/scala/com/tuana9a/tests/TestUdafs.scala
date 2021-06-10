package com.tuana9a.tests

import com.tuana9a.model.data.UserData
import com.tuana9a.process.udafs.{AverageSalaryAgg, HighestSalaryAgg}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, first, max, udaf}

object TestUdafs {
  private val tests = new Array[() => Unit](100)

  def run(idx: Any): Unit = {
    tests(idx.toString.toInt)()
  }

  tests(0) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val salaryAverage = udaf(AverageSalaryAgg)
    val users = spark.read.parquet("user-data-0.parquet")
    users.select(salaryAverage(users.col("salary"))).show()
    users.select(avg("salary")).show()

    spark.stop()
  }

  tests(1) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val highestSalary = udaf(HighestSalaryAgg)
    val users = spark.read.parquet("user-data-0.parquet")
    users.select(highestSalary(users.col("salary"))).show()
    users.select(max("salary")).show()

    spark.stop()
  }

  tests(2) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    import spark.implicits._
    val users = spark.read.parquet("user-data-0.parquet").as[UserData]
    val highestSalary = udaf(HighestSalaryAgg)
    users.groupBy("country").agg(highestSalary(users.col("id"), users.col("salary"))).show()
    users.groupBy("country").agg(max("salary"), first("id")).show()

    spark.stop()
  }

  tests(3) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    import spark.implicits._
    val users = spark.read.parquet("user-data-0.parquet").as[UserData]
    val highestSalary = udaf(HighestSalaryAgg)
    users.select(highestSalary(users.col("id"), users.col("salary"))).show()
    users.select(max("salary"), first("id")).show()

    spark.stop()
  }

  tests(4) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    spark.stop()
  }

}
