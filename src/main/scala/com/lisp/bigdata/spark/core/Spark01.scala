package com.lisp.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01 {
  //1.创建SparkConf并设置App名称
  val conf = new SparkConf().setMaster("local[*]").setAppName("WC")

  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc = new SparkContext(conf)

  private val value: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))
  value.map(x=>x)
  val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
  rdd.partitionBy(new org.apache.spark.HashPartitioner(2))

}
