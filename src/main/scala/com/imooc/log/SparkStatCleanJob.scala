package com.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * 使用Spark完成我们的数据清洗操作
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.parquet.compression.codec","gzip")//压缩方式
      .master("local[2]").getOrCreate()
    val accessRDD = spark.sparkContext.textFile("E:\\TestFile\\logs\\access1.log")

    //RDD => DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    accessDF.printSchema()
    accessDF.show(false)
    //coalesce:指定输出一个文件
    //mode:重写模式
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day").save("E:\\TestFile\\logs\\clean2")

    spark.stop()
  }

}
