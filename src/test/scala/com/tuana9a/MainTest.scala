package com.tuana9a

import com.tuana9a.model.data.UserData
import com.tuana9a.udafs.TestAggs.{CaseClass2, CaseClass3, CaseClass4}
import com.tuana9a.udafs.{HighestSalaryAgg, TestAggs}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{first, max, udaf}
import org.junit._

import java.io.{BufferedWriter, FileInputStream, FileWriter}
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.io.Source

class MainTest {

  @Test
  def test0(): Unit = {
    try {
      val config1 = new Properties()
      config1.load(new FileInputStream("config1.properties"))
      config1.forEach((k, v) => println(s"${k}=${v}"))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    println("read config2")
    try {
      val config2 = new Properties()
      config2.load(new FileInputStream("config.properties"))
      config2.forEach((k, v) => println(s"${k}=${v}"))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    println("write csv")
    try {
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
      fileSource.getLines().foreach(line => println("read: " + line))
      fileSource.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  //  @Test
  def test1(): Unit = {
    def fun(s: String*): Unit = {
      s.foreach(println)
    }

    val temp: ListBuffer[String] = new ListBuffer[String]
    temp += "tuan"
    temp += "tuan1"
    temp += "tuan2"
    fun(temp: _*)

    case class Test(name: String)
    val t = Test("tuan11")
    println(t.name.equals("tuan01"))
  }

  //  @Test
  def test2(): Unit = {
    val list1 = new ListBuffer[String]
    list1 += "10"
    list1 += "9"
    list1 += "8"
    val list2 = Seq("10", "9", "8")
    assert(list1.contains("10"))
    assert(list2.contains("10"))
    assert(list1.contains("8"))
    assert(!list1.contains("5"))

    case class T(var x1: Int, var x2: Int)
    val t1 = T(1, 2)
    val t2 = T(2, 1)
    t1.x1 += t2.x1
    t1.x2 += t2.x2
    println(t1)

  }

  //  @Test
  def test3(): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    import spark.implicits._

    val users = spark.read.parquet("/mnt/sda1/RESOURCE/parquet/user-data-0.parquet").as[UserData]
    val highestSalary = udaf(HighestSalaryAgg)

    users.select(highestSalary($"id", $"salary")).show()
    users.select(max($"salary"), first("id")).show()

    users.groupBy("country").agg(highestSalary($"id", $"salary").as("first_name")).as[UserData].show()

    spark.stop()
  }

  //  @Test
  def test4(): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    import spark.implicits._

    val data1 = Seq(
      (1, "tuan1"),
      (2, "tuan2"),
      (3, "tuan3"),
      (4, "tuan4"),
    ).toDF("id", "name")

    val data2 = Seq(
      //      (1, "tuan1"),
      (2, "tuan2"),
      (3, "tuan3"),
      //      (4, "tuan4"),
    ).toDF("id", "name")

    data1.join(data2, Seq("id"), "inner").show()
    data1.join(data2, Seq("id"), "left").show()
    data1.join(data2, Seq("id"), "leftanti").show()

    spark.stop()
  }

  //  @Test
  def test5(): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()

    val itemPrice = List(("Soap", 10.0), ("Toaster", 200.0), ("Tshirt", 400.0))
    val itemRDD = spark.sparkContext.makeRDD(itemPrice)

    val dummyItem = ("dummy", 0.0);
    val maxPrice = itemRDD.fold(dummyItem)((acc, item) => {
      if (acc._2 < item._2) item else acc
    })
    println("maximum price item " + maxPrice)

    val itemDepPrice = List(("gen", ("Soap", 10.0)), ("elect", ("Toaster", 200.0)), ("app", ("Tshirt", 400.0)))
    val itemDepRDD = spark.sparkContext.makeRDD(itemDepPrice)

    val maxPriceDept = itemDepRDD.foldByKey(("dummy", 0.0))((acc, element) => if (acc._2 > element._2) acc else element)
    println("maximum price item by dept " + maxPriceDept.collect().toList)
  }

  //  @Test
  def test6(): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    import spark.implicits._

    val users = spark.read.parquet("/mnt/sda1/RESOURCE/parquet/user-data-0.parquet").as[UserData]
    users.collect().foreach(println)
  }

  //  @Test
  def test7(): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val data1 = Seq(
      (0, 1, 1),
      (0, 1, 1),
      (0, 1, 1),
      (1, 1, 1),
      (1, 1, 1),
      (1, 1, 1),
    ).toDF("id", "x1", "x2")
    val agg1 = udaf(TestAggs.Aggregator1)
    val data2 = data1.groupBy("id").agg(agg1($"x1", $"x2")).as[(Int, TestAggs.CaseClass1)]

    data2.collect().foreach(x => {
      println(x._1, x._2.x1, x._2.x2)
    })

    println("======================================3=============================================")
    val agg2 = udaf(TestAggs.Aggregator2)
    val data3 = data1.groupBy("id").agg(agg2($"id", $"x1", $"x2").as("list")).as[CaseClass3]

    data3.collect().foreach(x => {
      x.list.foreach(c => {
        println(x.id, c.id, c.x1, c.x2)
      })
    })
    data3.collect().foreach(println)

    println("=====================================4==============================================")
    val agg3 = udaf(TestAggs.Aggregator3)
    val data4 = data1.groupBy("id")
      .agg(agg3($"id", $"x1", $"x2").as("caseClass2"))
      .as[CaseClass4]

    data4.collect().foreach(println)
    data4.select($"id", $"caseClass2".as[CaseClass2]).show()


    println("====================================5===============================================")
    val agg4 = udaf(TestAggs.GroupListCaseClass2)
    val data5 = data1.groupBy("id")
      .agg(agg4($"id", $"x1", $"x2").as("list"))
      .as[CaseClass3]
    data5.collect().foreach(x => {
      x.list.foreach(c => {
        println(x.id, c.id, c.x1, c.x2)
      })
    })

  }

  @Test
  def test8(): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    println("read config1")
    try {
      val config1 = new Properties()
      config1.load(new FileInputStream("config1.properties"))
      config1.forEach((k, v) => println(s"${k}=${v}"))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    println("read config2")
    try {
      val config2 = new Properties()
      config2.load(new FileInputStream("config.properties"))
      config2.forEach((k, v) => println(s"${k}=${v}"))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    println("write csv")
    try {
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
      fileSource.getLines().foreach(line => println("read: " + line))
      fileSource.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    spark.stop()
  }
}


