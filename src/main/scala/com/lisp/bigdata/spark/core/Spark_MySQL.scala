package com.lisp.bigdata.spark.core

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_MySQL {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)
    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "000000"

    //创建JdbcRDD
    val rdd = new JdbcRDD(sc,
      () => {
          Class.forName(driver)
          DriverManager.getConnection(url, userName, passWd)
        },
      "select id, name from user where id>=? and id <=?;",
      2,
      2,
      1,
      r => {println(r.getInt(1)+ "," + r.getString(2))}

//        (r.getInt(1), r.getString(2))
    )

    //打印最后结果
    println(rdd.count())
    rdd.collect()


    sc.stop()
  }

}
