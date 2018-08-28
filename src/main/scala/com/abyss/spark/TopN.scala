package com.abyss.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Abyss on 2018/8/26.
  * description:
  */
object TopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UV").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val result = sc.textFile("./data/access.log")
      //每行进行切分
      .map(_.split(" "))
      //滤去丢失字段和空的url的数据
      .filter(x => x.length > 10 && x(10) != "\"-\"")
      //获取url计数为1
      .map(x => (x(10), 1))
      //统计每个url的访问次数
      .reduceByKey(_ + _)
      //按次数进行降序排列
      .sortBy(_._2, false)
      //取前5
      .take(5)

    for (x <- result) {
      println(x._1 + "--------------" + x._2)
    }
    sc.stop()
  }

}
