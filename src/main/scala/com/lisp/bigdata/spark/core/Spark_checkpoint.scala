package com.lisp.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_checkpoint {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("practice")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint_")
    val log: RDD[String] = sc.textFile("in\\agent.log")
    val ch: RDD[String] = log.map(_ + System.currentTimeMillis)
    ch.checkpoint()

    val strings: Array[String] = ch.take(10)

    strings.foreach(println)
    println(ch.toDebugString)
    sc.stop()
  }

}
