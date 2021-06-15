package com.tuana9a.getdata

import org.apache.spark.sql.{Dataset, SparkSession}

class WordCountGetDataHdfs(spark: SparkSession) extends GetDataHdfs[String](spark){
  override def getData(path: String): Dataset[String] = {
    val textFile = spark.read.textFile(path)
    textFile
  }
}
