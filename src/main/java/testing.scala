package com.temp

import java.util

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object testing {
  def add_tmp(a: Int, b: Int): Int = {
    val sum = a + b
    return sum
  }

  def hello(a: java.lang.String): java.lang.String = {
    val result = "HELL " + a
    result.asInstanceOf[java.lang.String]
  }

  def get_params(param_list: java.util.Map[java.lang.String, java.lang.Double]): java.util.Map[java.lang.String, java.lang.String] = {
    val modified_param_list = param_list.toMap.transform((k, v) => String.format("%.3f", v))

    mapAsJavaMap(modified_param_list).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
  }

  def basic_transform_rdd(a : JavaRDD[Int]) : JavaRDD[Int] = {
    val b = a.rdd.map(a => a + 1)
    b.toJavaRDD()
  }

  def get_two_elems(a: JavaRDD[Int], param_list: java.util.Map[java.lang.String, java.lang.Double]): java.util.List[Object] = {
    val result_1 = basic_transform_rdd(a)
    val result_2 = get_params(param_list)

    val result = ArrayBuffer(result_1, result_2).asJava
    result
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("none")
      .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(config = conf)
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(Seq(1, 2, 3, 4, 5))

    val p1Ratings = mapAsJavaMap(
      Map("Lady in the Water" -> 3.0,
        "Snakes on a Plane" -> 4.0,
        "You, Me and Dupree" -> 3.5
      )
    ).asInstanceOf[java.util.Map[java.lang.String, java.lang.Double]]

    println(p1Ratings)

    val tempVar = p1Ratings.toMap

    println(tempVar)

    val modified_temp_var = tempVar.transform((k, v) => String.format("%.3f", v))
    //
    println(modified_temp_var)

    //    print(hello("yo".asInstanceOf[java.lang.String]))

    println(get_params(p1Ratings))

    println(get_two_elems(sc.parallelize(Seq(1,2,3,4)).toJavaRDD(), p1Ratings))

  }


}
