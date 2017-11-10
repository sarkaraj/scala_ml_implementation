package com.temp

import org.apache.spark.{SparkConf, SparkContext}

object testing {
  def add_tmp(a: Int, b:Int) : Int = {
    val sum = a + b
    return sum
  }

  def hello(a:java.lang.String) : java.lang.String = {
    val result = "HELL " + a
    result.asInstanceOf[java.lang.String]
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("none")
      .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(config = conf)
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(Seq(1,2,3,4,5))

    print(hello("yo".asInstanceOf[java.lang.String]))

  }


}
