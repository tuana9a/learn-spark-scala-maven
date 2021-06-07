package com.tuana9a.tests

import com.tuana9a.Main
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TestAdmicro {
  private val tests = new Array[() => Unit](100)

  def run(idx: Any): Unit = {
    tests(idx.toString.toInt)()
  }

  tests(0) = () => {
    //EXPLAIN: đếm xem có bao nhiêu user trong một domain
    val spark = SparkSession.builder.appName("test").getOrCreate()
    val data = spark.read.parquet("/Data/Logging/pageviewLog/pc/2021-05-26")
    data.createOrReplaceTempView("pageviewlog")
    spark.sql("SELECT domain, count(*) as user_count FROM (SELECT domain, guid FROM pageviewlog GROUP BY domain, guid) GROUP BY domain ORDER BY user_count DESC LIMIT 10").show()
    data.groupBy("domain").agg(countDistinct("guid").as("user_count")).orderBy(desc("user_count")).show(10)
    spark.stop()
  }

  tests(1) = () => {
    //CAUTION: sida vl
    val spark = SparkSession.builder.appName("test").getOrCreate()
    val data = spark.read.parquet("/Data/Logging/pageviewLog/pc/2021-05-26")
    val user_access_any_count_sorted = data.groupBy("guid").agg(count("*").as("access_count")).orderBy("access_count")
    val user_access_domain_count = data.groupBy("guid", "domain").agg(count("*").as("user_access_domain_count"))
    val top1_user_access_domain = user_access_domain_count.orderBy("user_access_domain_count").groupBy("domain").agg(last("guid").as("guid"))
    top1_user_access_domain.join(user_access_any_count_sorted, "guid").orderBy(desc("access_count")).show()
    top1_user_access_domain.join(user_access_any_count_sorted, "guid").orderBy(desc("access_count")).groupBy("guid").agg(first("domain")).show()
    spark.stop()
  }

  tests(2) = () => {
    //EXPLAIN: những người xem cả 2 path
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val data = spark.read.parquet("/Data/Logging/pageviewLog/pc/2021-05-26")
    val path = "/nguoi-dan-vung-lu-ha-tinh-noi-ve-khoan-137-ty-quyen-gop-cua-ns-hoai-linh-khi-nuoc-rut-duoc-vai-ngay-la-luc-chung-toi-can-cuu-tro-nhat-mot-nam-khi-doi-bang-mot-goi-khi-no-20210526035409185.chn"
    val domain = "kenh14.vn"
    val visit_path_condition = s"path='$path' and domain='$domain'"

    val user_see_path = data.filter(visit_path_condition).groupBy("guid").agg(first("dt").as("dt_path"))
    val user_see_domain = user_see_path.join(data, "guid")
    val user_see_domain_count = user_see_domain.groupBy("guid").agg(count("*").as("domain_view"))

    val see_after_path_condition = user_see_domain.col("dt").gt(user_see_path.col("dt_path"))
    val user_see_domain_after_path = user_see_domain.filter(see_after_path_condition)
    val user_see_domain_after_path_count = user_see_domain_after_path.groupBy("guid").agg(count("*").as("domain_after_path_view"))

    //CAUTION: inner join
    user_see_domain_count.join(user_see_domain_after_path_count, "guid").show()
    spark.stop()
  }

  tests(3) = () => {
    //EXPLAIN: những người xem xem path2 sau path1
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val data = spark.read.parquet("/Data/Logging/pageviewLog/pc/2021-05-26")

    val path1 = "/nguoi-dan-vung-lu-ha-tinh-noi-ve-khoan-137-ty-quyen-gop-cua-ns-hoai-linh-khi-nuoc-rut-duoc-vai-ngay-la-luc-chung-toi-can-cuu-tro-nhat-mot-nam-khi-doi-bang-mot-goi-khi-no-20210526035409185.chn"
    val path2 = "/cap-chi-em-sinh-doi-thien-than-duoc-menh-danh-dep-nhat-dai-loan-gay-ngo-ngang-voi-dien-mao-o-tuoi-thieu-nu-sau-16-nam-2021052518053709.chn"
    val domain = "kenh14.vn"

    //SECTION: cách 1
    val visit_path_condition = s"(path='$path1' or path='$path2') and domain='$domain'"
    val user_see_path = data.filter(visit_path_condition)
    val user_see_path_count = user_see_path.groupBy("guid").agg(countDistinct("path").as("both_check"))

    val visit_both_condition = "both_check = 2"
    val user_see_both_path_count = user_see_path_count.filter(visit_both_condition)
    val user_see_both_path = user_see_both_path_count.join(user_see_path, "guid")

    val user_see_first_path = user_see_both_path_count.join(user_see_path, "guid").groupBy("guid").agg(min("dt").as("dt_start"))
    val user_see_first_path_value = user_see_both_path.join(user_see_first_path, "guid")

    val user_see_seq_path = user_see_first_path_value.filter(s"path = '$path1'")

    user_see_seq_path.select("guid").show()

    //SECTION: cách 2
    val visit_path1_condition = s"path='$path1' and domain='$domain'"
    val user_see_path1 = data.filter(visit_path1_condition).groupBy("guid").agg(first("path").as("path1"), first("dt").as("dt_1"))

    val visit_path2_condition = s"path='$path2' and domain='$domain'"
    val user_see_path2 = data.filter(visit_path2_condition).groupBy("guid").agg(first("path").as("path2"), first("dt").as("dt_2"))

    val join_condition = user_see_path2.col("dt_2").gt(user_see_path1.col("dt_1"))
    val user_see2_after_see1 = user_see_path1.join(user_see_path2, join_condition)
    user_see2_after_see1.show()
    user_see2_after_see1.count()

    spark.stop()
  }

  tests(4) = () => {
    //EXPLAIN: những người xem path1 mà không xem path2
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val data = spark.read.parquet("/Data/Logging/pageviewLog/pc/2021-05-26")

    val path1 = "/nguoi-dan-vung-lu-ha-tinh-noi-ve-khoan-137-ty-quyen-gop-cua-ns-hoai-linh-khi-nuoc-rut-duoc-vai-ngay-la-luc-chung-toi-can-cuu-tro-nhat-mot-nam-khi-doi-bang-mot-goi-khi-no-20210526035409185.chn"
    val path2 = "/cap-chi-em-sinh-doi-thien-than-duoc-menh-danh-dep-nhat-dai-loan-gay-ngo-ngang-voi-dien-mao-o-tuoi-thieu-nu-sau-16-nam-2021052518053709.chn"
    val domain = "kenh14.vn"

    val visit_path1_condition = s"path='$path1' and domain='$domain'"
    val user_see_path1 = data.filter(visit_path1_condition).groupBy("guid").agg(first("path").as("path1"))

    val visit_path2_condition = s"path='$path2' and domain='$domain'"
    val user_see_path2 = data.filter(visit_path2_condition).groupBy("guid").agg(first("path").as("path2"))

    val see1_left_join_see2 = user_see_path1.join(user_see_path2, Seq("guid"), "left")
    val see1_not_see2 = see1_left_join_see2.filter(see1_left_join_see2.col("path2").isNull)

    see1_not_see2.select("guid", "path1", "path2").show()
    see1_not_see2.count()

    data.filter(s"guid = 1081650152883629548 and (path='$path1' or path='$path2')").select("guid", "path").show()

    spark.stop()
  }

  tests(5) = () => {
    //EXPLAIN: thời gian dùng kenh14 theo giây//SECTION: cách 1
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val data = spark.read.parquet("/Data/Logging/pageviewLog/pc/2021-05-26")
    val domain = "kenh14.vn"

    val user_kenh14_condition = s"domain = '$domain'"
    val user_kenh14 = data.filter(user_kenh14_condition)

    val user_start = user_kenh14.groupBy("guid").agg(min("dt").as("dt_start"))
    val user_stop = user_kenh14.groupBy("guid").agg(max("dt").as("dt_stop"))

    var user_start_stop = user_start.join(user_stop, "guid")

    user_start_stop = user_start_stop.withColumn("dt_start", user_start_stop.col("dt_start").cast("timestamp"))
    user_start_stop = user_start_stop.withColumn("dt_stop", user_start_stop.col("dt_stop").cast("timestamp"))

    val delta = user_start_stop.col("dt_stop").cast("long") - user_start_stop.col("dt_start").cast("long")
    val user_duration = user_start_stop.withColumn("delta_secs", delta).orderBy(desc("delta_secs"))

    user_duration.show()

    spark.stop()
  }

  tests(6) = () => {
    //EXPLAIN: thời gian dùng kenh14 theo giây SECTION: cách 2
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val data = spark.read.parquet("/Data/Logging/pageviewLog/pc/2021-05-26")
    val domain = "kenh14.vn"

    val user_kenh14_condition = s"domain = '$domain'"
    val user_kenh14 = data.filter(user_kenh14_condition)

    val dt_start = min("dt").cast("timestamp").cast("long")
    val dt_stop = max("dt").cast("timestamp").cast("long")
    val sub = dt_stop - dt_start

    val user_duration = user_kenh14.groupBy("guid").agg(sub.as("delta_secs"))

    user_duration.orderBy(desc("delta_secs")).show()

    spark.stop()
  }

  tests(7) = () => {
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val data = spark.read.parquet("/Data/Logging/pageviewLog/pc/2021-05-26")
    val domain = "kenh14.vn"

    val user_kenh14_condition = s"domain = '$domain'"
    val user_kenh14 = data.filter(user_kenh14_condition)

    val user_dt_stop = data.groupBy("guid").agg(max("dt").as("dt"))
    val user_stop_path = user_dt_stop.join(data, Seq("guid", "dt"))

    val stop_path_count = user_stop_path.groupBy("path").agg(count("*").as("count"))
    val top_stop_path = stop_path_count.orderBy(desc("count"))

    top_stop_path.show()

    spark.stop()
  }

  tests(8) = () => {
    //EXPLAIN: tìm google_guid còn thiếu
    val spark = SparkSession.builder.appName("test").getOrCreate()

    val data = spark.read.parquet("/Data/Logging/pageviewLog/mob/2021-05-30")

    val google_data = spark.read.textFile("user_mob_ga_30_05.txt").toDF("google_guid")
    val admicro_data = data.groupBy("guid").agg(first("screen").as("screen"))

    val get_google_guid = substring(admicro_data.col("screen"), 7, 100)
    val admicro_data_google = admicro_data.withColumn("google_guid", get_google_guid)

    val google_left_join_admicro = google_data.join(admicro_data_google, Seq("google_guid"), "left")
    val admicro_guid_null = google_left_join_admicro.col("guid").isNull
    val google_guid_missing = google_left_join_admicro.filter(admicro_guid_null)

    google_guid_missing.count()
    google_guid_missing.select("guid", "google_guid").show()

    admicro_data_google.select("guid", "screen", "google_guid").take(1).foreach(println)
    google_left_join_admicro.select("guid", "screen", "google_guid").take(1).foreach(println)

    spark.stop()
  }
}
