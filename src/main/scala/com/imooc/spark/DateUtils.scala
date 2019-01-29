package com.imooc.spark

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间解析工具类
  * 注意：SimpleDateFormat是线程不安全的
  */
object DateUtils {
  //2019-01-25 14:21:32
//  val YYYYMMDDHHMM_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
//  val TARGET_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy年MM月dd日 HH时mm分ss秒")


  /**
    * 获取时间
    * @param time
    * @return
    */
  def parse(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def getTime(time: String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time).getTime
    }catch{
      case e: Exception =>{
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("2019-01-25 14:21:32"))
  }
}
