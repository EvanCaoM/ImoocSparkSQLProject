package com.imooc.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
/**
  * TopN统计Spark作业
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("E:\\TestFile\\logs\\clean")

    accessDF.printSchema()
    accessDF.show(false)

    val day = "20190126"
    //删除插入日期的数据
    StatDAO.deleteData(day)

    //最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF, day)

    //按照地市进行统计TopN课程
    cityAccessTopNStat(spark, accessDF, day)

    //按照流量进行统计
    videoTrafficsTopNStat(spark, accessDF, day)

    spark.stop()
  }

  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day:String): Unit = {
    /**
      * 使用DataFrame的方式进行统计
      */
    import spark.implicits._
    val videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "class")
      //需要导入函数包
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    videoAccessTopNDF.show(false)

    /**
      * 使用SQL的方式进行统计
      */
//    accessDF.createOrReplaceTempView("access_logs")
//    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs"
//      +" where day='20190126' and cmsType='class' group by day,cmsId order by times desc")
//    videoAccessTopNDF.show(false)


    /**
      * 将统计结果写入到MySql中
      */
    try {
      videoAccessTopNDF.foreachPartition(partitonOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitonOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })
        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }


  }

  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day:String): Unit = {
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "class")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))
//    cityAccessTopNDF.show(false)

    //window函数在Spark SQL的使用
    val top3DF = cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
      .orderBy(cityAccessTopNDF("times").desc))
    .as("times_rank"))
      .filter("times_rank <= 3")//.show(false) //Top 3

    /**
      * 将统计结果写入到MySql中
      */
    try {
      top3DF.foreachPartition(partitonOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitonOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day:String): Unit = {
    import spark.implicits._
    val videoTrafficsTopNDF = accessDF.filter($"day" === day && $"cmsType" === "class")
      .groupBy("day", "cmsId")
      .agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)//.show(false)

    try{
      videoTrafficsTopNDF.foreachPartition(partitonOfRecords =>{
        val list = new ListBuffer[DayVideoTrafficsStat]
        partitonOfRecords.foreach(info =>{
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })
        StatDAO.insertDayVideoTrafficsTopN(list)
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }
}
