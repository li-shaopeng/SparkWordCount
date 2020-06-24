package com.lisp.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object practice {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("practice")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val log: RDD[String] = sc.textFile("in\\agent.log")
    val cityAndAdv: RDD[(String, String)] = log.map(x => {
      val strings: Array[String] = x.split(" ")
      (strings(1), strings(4))
    })
    //方法1，采用groupBy
    val city_advGY: RDD[((String, String), Iterable[(String, String)])] = cityAndAdv.groupBy(x => x)
    val city_advNUM: RDD[((String, String), Int)] = city_advGY.map(x => {
      var advSum: Int = 0
      x._2.foreach(x => {
        advSum = advSum + 1
      })
      (x._1, advSum)
    })

//    val outTuple: Array[((String, String), Int)] = city_advNUM.sortBy(x => x._2,false).take(3)
//    outTuple.foreach(println)

    //方法2，采用Key-Value类型
  //  cityAndAdv.combineByKey
    println(s"_______________________________________")
    val cityAdvCount: RDD[((String, String), Int)] = cityAndAdv.map(x =>(x,1))
    val provinceToAd: RDD[((String, String), Int)] = cityAdvCount.reduceByKey(_+_)
    val provinceToAdSum: RDD[(String, (String, Int))] = provinceToAd.map(x => (x._1._1, (x._1._2, x._2)))
    val provinceGroup : RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues(x => {
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    })
    provinceAdTop3.collect().foreach(println)
//    val CountAll: RDD[((String, String), Int)] = cityAdvCount.aggregateByKey(0)(_+_,_+_)
//    val tuples: Array[((String, String), Int)] = CountAll.sortBy(x => x._2,false).take(3)
  //  private val value: RDD[String] = log.flatMap(_.split(" "))
    sc.stop()
  }

}
