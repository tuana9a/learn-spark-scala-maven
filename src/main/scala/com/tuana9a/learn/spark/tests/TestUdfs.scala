package com.tuana9a.learn.spark.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object TestUdfs {
  private val tests = new Array[() => Unit](100)

  def run(idx: Any): Unit = {
    tests(idx.toString.toInt)()
  }

  tests(0) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val strlen = udf((s: String) => s.length)
    val users = spark.read.parquet("user-data-0.parquet")
    //    users.select("id", strlen(users.col("first_name")), "gender").show()
    val first_name_strlen = users.withColumn("first_name_length", strlen(users.col("first_name")))
    first_name_strlen.select("first_name", "first_name_length").show()

    spark.stop()
  }

  tests(1) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    spark.stop()
  }

}
