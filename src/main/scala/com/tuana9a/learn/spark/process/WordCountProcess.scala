package com.tuana9a.learn.spark.process

import org.apache.spark.sql.{Dataset, SparkSession}


class WordCountProcess(spark: SparkSession) extends BaseProcess[String](spark) {

  def process(dataset: Dataset[String]): Dataset[String] = {
    import spark.implicits._
    val wordCount = dataset.rdd.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((c1, c2) => c1 + c2)
    //    wordCount.collect.foreach(wc => printf("%-20s    %d\n", wc._1, wc._2))
    val result = wordCount.map(p => s"${p._1}   ---   ${p._2}").toDS()
    result
  }

}
