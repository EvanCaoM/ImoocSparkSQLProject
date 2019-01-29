package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：抽取出我们所需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob")
      .master("local[1]").getOrCreate()

    val access = spark.sparkContext.textFile("E:\\TestFile\\logs\\access.log")
    //access.take(10).foreach(println)
    access.map(line => {
      val splits = line.split('\t')
      val ip = splits(0)
      val time = splits(1)
      val url = splits(2).substring(5).split(" ")(0)
      val traffic = splits(3)
//      (ip, DateUtils.parse(time), url, traffic)
      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("E:\\TestFile\\logs\\output\\")


    spark.stop()
  }
}
