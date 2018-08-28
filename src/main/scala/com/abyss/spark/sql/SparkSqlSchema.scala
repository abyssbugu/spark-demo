package com.abyss.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Abyss on 2018/8/28.
  * 通过StructType直接指定Schema
  * 当case class不能提前定义好时，可以通过以下三步创建DataFrame
  * [1]将RDD转为包含Row对象的RDD
  * [2]基于StructType类型创建schema，与第一步创建的RDD相匹配
  * [3]通过sparkSession的createDataFrame方法对第一步的RDD应用schema创建DataFrame
  */
//将RDD转化为DataFrame，利用structtype直接指定schema
object SparkSqlSchema {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark = SparkSession.builder().appName("CaseClassSchema").master("local[2]").getOrCreate()
    //2.获取SparkContext对象
    val sc = spark.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //3.通过sparkContext读取数据文件
    val dataRDD = sc.textFile("/Users/abyss/Dev/toys/people.txt")
    //4.切分每一行
    val lineArrayRDD = dataRDD.map(_.split(","))
    //5.可以将lineArrayRDD和样例类Person向关联
    val rowRDD = lineArrayRDD.map(x => Row(x(0).toInt, x(1), x(2).toInt))
    //6.通过structtype指定schema
    val structType = StructType(StructField("id", IntegerType, true) ::
      StructField("name", StringType, false) ::
      StructField("age", IntegerType, false) :: Nil)
    //7.通过使用sparkSession来调用createDataFrame来生成DataFrame
    val personDF = spark.createDataFrame(rowRDD, structType)

    //----------DSL操作----------------------
    //1.打印personDF中schema信息
    personDF.printSchema()

    //2.打印personDF中的数据，默认打印20条
    personDF.show()

    //-----------Sql风格-----------------------
    //先将personDF注册成表
    personDF.createOrReplaceTempView("t_person")

    //1.查看t_person表中的所有数据
    spark.sql("select * from t_person").show()

    //2.查看lisi的信息
    spark.sql("select * from t_person where name='lisi'").show()

    //3.查看t_person所有信息，按照年龄倒序
    spark.sql("select * from t_person order by age desc").show()

    //关闭
    sc.stop()
    spark.stop()

  }

}
