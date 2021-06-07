package com.tuana9a.tests

import com.tuana9a.{Helper, Main}
import com.tuana9a.model.UserData
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
    val data = Seq(
      UserData(Helper.genTimestamp(milliseconds), 1, "Tuan1", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
      UserData(Helper.genTimestamp(milliseconds + 1000), 2, "Tuan2", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
      UserData(Helper.genTimestamp(milliseconds + 2000), 3, "Tuan3", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
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
    val data = Seq(
      UserData(Helper.genTimestamp(milliseconds), 5, "Tuan1", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
      UserData(Helper.genTimestamp(milliseconds + 1000), 6, "Tuan2", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
      UserData(Helper.genTimestamp(milliseconds + 2000), 7, "Tuan3", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
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

    val data = Seq(
      UserData(Helper.genTimestamp((milliseconds - Math.random() * 604800000).toLong), 1, "Tuan1", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
      UserData(Helper.genTimestamp((milliseconds - Math.random() * 604800000).toLong), 2, "Tuan2", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
      UserData(Helper.genTimestamp((milliseconds - Math.random() * 604800000).toLong), 3, "Tuan3", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
    )
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF()
    df.write.mode("overwrite").parquet("user-data-1.parquet")

    for (i <- Range(0, 100)) {
      val data = Seq(
        UserData(Helper.genTimestamp((milliseconds - Math.random() * 604800000).toLong), 1, "Tuan1", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
        UserData(Helper.genTimestamp((milliseconds - Math.random() * 604800000).toLong), 2, "Tuan2", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
        UserData(Helper.genTimestamp((milliseconds - Math.random() * 604800000).toLong), 3, "Tuan3", "Nguyen Minh", "tuana9a@gmail.com", "Male", "192.168.244.128", null, "Hanoi", null, null, null, null),
      )
      val rdd = spark.sparkContext.parallelize(data)
      val df = rdd.toDF()
      df.write.mode("append").parquet("user-data-1.parquet")
    }
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
        val random = (milliseconds - Math.random() * 604800000).toLong
        val timestamp = Helper.genTimestamp(random)
        val id = Helper.genId(random)
        temp = temp :+ UserData(timestamp, id, "Tuan" + id, null, null, null, null, null, null, null, null, null, null)
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
        val random = (milliseconds - Math.random() * 604800000).toLong
        val timestamp = Helper.genTimestamp(random)
        val id = Helper.genId(random)
        data = data :+ UserData(timestamp, id, "Tuan" + id, null, null, null, null, null, null, null, null, null, null)
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
    println("start1")
    users.take(10).foreach(u => {
      println(u.id, u.registration_dttm)
    })
    println("start2")
    users.take(10).foreach(println)
    spark.stop()
  }
}
