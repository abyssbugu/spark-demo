package com.abyss.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Abyss on 2018/8/26.
  * 计算网站用户访问总量---PV
  */
object PV {
  def main(args: Array[String]): Unit = {
    //spark配置文件,本地计算
    val conf = new SparkConf().setAppName("PV").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val pv = sc.textFile("/Users/abyss/Dev/Demos/spark-wordcount/access.log").count()
    println(pv)
    sc.stop()

  }

}
