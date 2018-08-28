package com.abyss.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Abyss on 2018/8/26.
  * 利用spark分析点击流日志数据------UV总量
  */
object UV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UV").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val uv = sc.textFile("./data/access.log")
      //获取ip地址集
      .map(_.split(" ")(0))
      //根据ip地址去重
      .distinct()
      //获得UV总量
      .count()
    println(uv)

  }

}
