package com.abyss.spark.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by Abyss on 2018/8/28.
  * description:
  */
//利用sparkSql从mysql中加载数据
object DataFromMysql {
  def main(args: Array[String]): Unit = {
    //1.创建sparSession对象
    val spark = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .enableHiveSupport() //开启对hive的支持
      .getOrCreate()

    //2.通过sparkSession获取mysql中的数据
    //准备配置属性
    val properties = new Properties()
    //设置用户名和密码
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")

    val dataFrame = spark.read.jdbc("jdbc:mysql://node1:3306/spark", "iplocation", properties)

    //打印dataFrame的schema信息
    dataFrame.printSchema()

    //打印dataFrame的数据
    dataFrame.show()

    //关闭
    spark.stop()


  }

}
