package com.tuana9a.learn.spark.tests

import com.tuana9a.learn.spark.Helper
import com.tuana9a.learn.spark.model.data.UserData
import org.apache.spark.sql.SparkSession

object TestParquet {
  private val tests = new Array[() => Unit](100)

  def run(idx: Any): Unit = {
    tests(idx.toString.toInt)()
  }

  tests(0) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val users = spark.read.parquet("user-data-0.parquet")
    users.select("id", "first_name", "gender").show()

    spark.stop()
  }

  tests(1) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val users = spark.read.parquet("user-data-0.parquet")
    users.filter(row => row.getInt(1) < 10).show()

    spark.stop()
  }

  tests(2) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val users = spark.read.parquet("user-data-0.parquet")
    users.createOrReplaceTempView("user")
    spark.sql("SELECT * FROM user WHERE first_name > 'mmmm'").show()

    spark.stop()
  }

  tests(3) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val users = spark.read.parquet("user-data-0.parquet")
    users.createOrReplaceTempView("user")
    spark.sql("SELECT * FROM user WHERE gender = 'Male'").write.parquet("user-data-male.parquet")

    spark.stop()
  }

  tests(4) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    import spark.implicits._

    val milliseconds = System.currentTimeMillis()
    val timestamp1 = Helper.genTimestamp(milliseconds)
    val timestamp2 = Helper.genTimestamp(milliseconds + 1000)
    val timestamp3 = Helper.genTimestamp(milliseconds + 2000)
    val data = Seq(
      UserData(timestamp1, 1, "Tuan1"),
      UserData(timestamp2, 2, "Tuan2"),
      UserData(timestamp3, 3, "Tuan3"),
    )
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF()
    df.printSchema()
    df.write.parquet("user-data-2.parquet")

    spark.stop()
  }

  tests(5) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    import spark.implicits._

    val milliseconds = System.currentTimeMillis()
    val timestamp1 = Helper.genTimestamp(milliseconds)
    val timestamp2 = Helper.genTimestamp(milliseconds + 1000)
    val timestamp3 = Helper.genTimestamp(milliseconds + 2000)
    val data = Seq(
      UserData(timestamp1, 1, "Tuan1"),
      UserData(timestamp2, 2, "Tuan2"),
      UserData(timestamp3, 3, "Tuan3"),
    )
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF()
    df.printSchema()
    df.write.mode("append").parquet("user-data-1.parquet")

    spark.stop()
  }

  tests(6) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    import spark.implicits._

    val milliseconds = System.currentTimeMillis()
    val timestamp1 = Helper.genTimestamp(milliseconds)
    val timestamp2 = Helper.genTimestamp(milliseconds + 1000)
    val timestamp3 = Helper.genTimestamp(milliseconds + 2000)
    val data = Seq(
      UserData(timestamp1, 1, "Tuan1"),
      UserData(timestamp2, 2, "Tuan2"),
      UserData(timestamp3, 3, "Tuan3"),
    )
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF()
    df.write.mode("overwrite").parquet("user-data-1.parquet")

    for (i <- Range(0, 100)) {
      val timestamp1 = Helper.genTimestamp(milliseconds)
      val timestamp2 = Helper.genTimestamp(milliseconds + 1000)
      val timestamp3 = Helper.genTimestamp(milliseconds + 2000)
      val data = Seq(
        UserData(timestamp1, 1, "Tuan1"),
        UserData(timestamp2, 2, "Tuan2"),
        UserData(timestamp3, 3, "Tuan3"),
      )
      val rdd = spark.sparkContext.parallelize(data)
      val df = rdd.toDF()
      df.write.mode("append").parquet("user-data-1.parquet")
    }

    spark.stop()
  }

  tests(7) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    import spark.implicits._

    val milliseconds = System.currentTimeMillis()
    val data = Seq[UserData]()
    var rdd = spark.sparkContext.parallelize(data)
    for (_ <- Range(0, 100)) {
      var temp = Seq[UserData]()
      for (_ <- Range(0, 10)) {
        val random = (milliseconds - Math.random() * 604800000).toInt
        val timestamp = Helper.genTimestamp(random)
        val id = Helper.genId(random)
        temp = temp :+ UserData(timestamp, id, "Tuan" + id)
      }
      rdd = rdd.++(spark.sparkContext.parallelize(temp))
    }
    val df = rdd.toDF()
    df.repartition(1).write.mode("overwrite").parquet("user-data-1.parquet")

    spark.stop()
  }

  tests(8) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    import spark.implicits._

    val milliseconds = System.currentTimeMillis()
    var data = Seq[UserData]()
    for (_ <- Range(0, 100)) {
      for (_ <- Range(0, 10)) {
        val random = (milliseconds - Math.random() * 604800000).toInt
        val timestamp = Helper.genTimestamp(random)
        val id = Helper.genId(random)
        data = data :+ UserData(timestamp, id, "Tuan" + id)
      }
    }
    val df = spark.sparkContext.parallelize(data).toDF()
    df.repartition(1).write.mode("overwrite").parquet("user-data-2.parquet")

    spark.stop()
  }

  tests(9) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()
    import spark.implicits._
    val users = spark.read.parquet("user-data-0.parquet").as[UserData]
    users.take(10).foreach(u => {
      println(u.registration_dttm, u.id, u.email)
    })
    println("=======================")
    users.take(10).foreach(println)
    spark.stop()
  }
}
