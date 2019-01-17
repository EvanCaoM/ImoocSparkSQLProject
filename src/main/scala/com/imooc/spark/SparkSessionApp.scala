package com.imooc.spark

import org.apache.spark.sql.SparkSession


/**
  * SparkSession的使用
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    val people = spark.read.json("file:/G:/TestFile/people.json")
    people.show()

    spark.stop()
  }

}
