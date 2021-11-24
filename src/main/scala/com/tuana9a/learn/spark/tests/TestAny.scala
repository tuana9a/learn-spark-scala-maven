package com.tuana9a.learn.spark.tests

import com.tuana9a.learn.spark.model.data.PageViewLog
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, FileInputStream, FileWriter}
import java.util.Properties
import scala.io.Source

object TestAny {
  private val tests = new Array[() => Unit](100)

  def run(idx: Any): Unit = {
    tests(idx.toString.toInt)()
  }

  tests(0) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    println("Hello World!")
    spark.stop()
  }

  tests(1) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    val textFile = spark.read.textFile("alice.txt").cache()
    val numAs = textFile.filter(line => line.contains("a")).count()
    val numBs = textFile.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

  tests(2) = () => {
    val spark = new SparkContext(new SparkConf().setAppName("test"))
    val textFile = spark.textFile("alice.txt").cache()
    val numAs = textFile.filter(line => line.contains("a")).count()
    val numBs = textFile.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

  tests(3) = () => {
    val path = "alice.txt"
    val sparkContext = new SparkContext(new SparkConf().setAppName("WordCount"))
    val textFile = sparkContext.textFile(path)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((c1, c2) => c1 + c2)
    wordCount.collect.foreach(wc => printf("%-20s    %d\n", wc._1, wc._2))
    sparkContext.stop()
  }

  tests(4) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    import spark.implicits._
    val data = spark.read.parquet("/Data/Logging/pageviewLog/pc/2021-05-26").as[PageViewLog]
    data.rdd.map(x => (x.guid, x.dt)).filter(x => x._1 < "tuan").groupByKey()
    spark.stop()
  }

  tests(5) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    def run(f: () => Unit): Unit = {
      try {
        f()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    run(() => {
      println("read config1")
      val config1 = new Properties()
      config1.load(new FileInputStream("config1.properties"))
      config1.forEach((k, v) => println(s"${k}=${v}"))
    })
    run(() => {
      println("read config2")
      val config2 = new Properties()
      config2.load(new FileInputStream("config.properties"))
      config2.forEach((k, v) => println(s"${k}=${v}"))
    })
    run(()=> {
      val filename = "test.csv"
      val writer: FileWriter = new FileWriter(filename)
      val bw: BufferedWriter = new BufferedWriter(writer)
      Seq((1, "tuan1"), (2, "tuan2"), (3, "tuan3")).foreach(x => {
        val line = x._1.toString + "," + x._2 + "\n"
        bw.write(line)
        println("wrote: " + line)
      })
      bw.flush()
      bw.close()

      val fileSource = Source.fromFile(filename)
      while (fileSource.hasNext) {
        println("read: " + fileSource.next)
      }
      fileSource.close()
    })
    spark.stop()
  }
}
