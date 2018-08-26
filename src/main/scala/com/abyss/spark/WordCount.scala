package com.abyss.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Abyss on 2018/8/26.
  * description:
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext()
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))
    sc.stop()
  }


}
