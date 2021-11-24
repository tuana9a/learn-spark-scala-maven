package com.tuana9a.learn.spark.getdata

import org.apache.spark.sql.{Dataset, SparkSession}

abstract class GetDataHdfs[OUT](spark: SparkSession) {
  def getData(path: String) : Dataset[OUT]
}
