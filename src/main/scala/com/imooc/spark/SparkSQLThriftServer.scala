package com.imooc.spark

import java.sql.DriverManager


/**
  * 通过JDBC的方式访问
  */
object SparkSQLThriftServer {


  def main(args: Array[String]): Unit = {


    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://evan:10000","evan","950513")
    val pstmt = conn.prepareStatement("select empno, ename, sal from emp")
    val rs = pstmt.executeQuery()
    while (rs.next()){
      println("empno:" + rs.getInt("empno") +
        " ,ename" + rs.getString("ename"))
    }
  }
}
