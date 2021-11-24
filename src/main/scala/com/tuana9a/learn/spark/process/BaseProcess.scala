package com.tuana9a.learn.spark.process

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Created by robert on 13/05/2017.
 */
abstract class BaseProcess[OUT](spark: SparkSession) {

  def process(datasets: Dataset[Any]*): Dataset[OUT] = {
    null
  }

}
