package com.lisp.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_2 {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_2")
    //创建SparkConf()并设置App名称
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.read.json("data/people.json")
    // Displays the content of the DataFrame to stdout
    df.show()
    df.filter($"age" > 21).show()
    df.createOrReplaceTempView("persons")
    spark.sql("SELECT * FROM persons where age > 21").show()
    spark.stop()
  }
}
