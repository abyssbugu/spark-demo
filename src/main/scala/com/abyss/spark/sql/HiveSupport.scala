package com.abyss.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by Abyss on 2018/8/28.
  * description:
  */

//利用sparkSql操作hiveSql
object HiveSupport {
  def main(args: Array[String]): Unit = {

    //1.创建sparSession对象
    val spark = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .enableHiveSupport() //开启对hive的支持
      .getOrCreate()

    //2.获取sparkContext对象
    val sc = spark.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")

    //3.通过sparkSession操作hiveSql
    //创建一个hive表,表的结构及数据和元数据都会生成在项目文件列表中
//        spark.sql("create table if not exists student(id int,name string,age int)"+
//        "row format delimited fields terminated by ','")

    //加载数据到hive表里面
//        spark.sql("load data local inpath './data/student.txt' into table student")

    //查询表的结果数据（创建表和加载数据表中之后，注释掉表的创建和加载数据代码，依然可以查看到数据，因为元数据存在）
    spark.sql("select * from student").show()


    //关闭
    sc.stop()
    spark.stop()
  }
}
