package com.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * 使用Spark完成我们的数据清洗操作
  */
object SparkStatCleanJobYarn {
  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      println("Usage:SparkStatCleanJobYarn <inputPath> <outputPath>")
      System.exit(1)
    }
    val Array(inputPath, outputPath) = args

    val spark = SparkSession.builder()
//      .appName("SparkStatCleanJob")
//      .master("local[2]")
      .getOrCreate()
    val accessRDD = spark.sparkContext.textFile(inputPath)
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    accessDF.printSchema()
    accessDF.show(false)
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day").save(outputPath)

    spark.stop()
  }

}
