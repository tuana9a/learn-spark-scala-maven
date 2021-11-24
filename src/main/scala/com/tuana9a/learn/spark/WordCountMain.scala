package com.tuana9a.learn.spark

import com.tuana9a.learn.spark.getdata.WordCountGetDataHdfs
import org.apache.spark.sql.SparkSession

object WordCountMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val data = new WordCountGetDataHdfs(spark).getData("multi-line-800mb.txt")
    val result = new WordCountProcess(spark).process(data)
    result.collect().foreach(println)

    spark.stop()
  }
}
