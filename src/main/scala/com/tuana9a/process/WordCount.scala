package com.tuana9a.process

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def run(path: Any): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setAppName("WordCount"))
    val textFile = sparkContext.textFile(path.toString)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((c1, c2) => c1 + c2)
    wordCount.collect.foreach(wc => printf("%-20s    %d\n", wc._1, wc._2))
    sparkContext.stop()
  }
}
