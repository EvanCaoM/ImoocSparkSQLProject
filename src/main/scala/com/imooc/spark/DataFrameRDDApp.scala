package com.imooc.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameRDDApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    //RDD => DataFrame
//    inferReflection(spark)
    program(spark)

    spark.stop()
  }

  private def program(spark: SparkSession) = {
    import spark.implicits._
    val rdd = spark.sparkContext.textFile("file:/G:/TestFile/infos.txt")
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))
    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    val infoDF = spark.createDataFrame(infoRDD, structType)
    infoDF.printSchema()
    infoDF.show()
  }

  private def inferReflection(spark: SparkSession) = {
    //注意：需要导入隐式转换
    import spark.implicits._
    val rdd = spark.sparkContext.textFile("file:/G:/TestFile/infos.txt")
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    infoDF.show()

    infoDF.filter(infoDF.col("age") > 30).show()
  }

  case class Info(id: Int, name: String, age: Int)
}
