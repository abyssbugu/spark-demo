package com.abyss.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by Abyss on 2018/8/28.
  * description:
  */
//定义一个样例类：Person
case class Person(id: Int, name: String, age: Int)

//将RDD---转换为-->DataFrame，利用反射机制 --- case class样例类
object CaseClassSchema {
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
    val personRDD = lineArrayRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    //6.将personRDD转化为DataFrame
    //需要手动导入隐式转换的包
    import spark.implicits._
    val personDF = personRDD.toDF()
    //--------------------------DSL语法操作----------------------------
    //1.显示DataFrame的schema信息
    personDF.printSchema()
    //2.显示DataFrame中的数据，默认显示20行
    personDF.show()
    //4.获取DataFrame的前3条数据
    val arr = personDF.head(3)
    arr.foreach(println(_))

    //5.获取DataFrame中的记录数
    println(personDF.count())
    //6.显示DataFrame中name字段的所有值
    personDF.select("name").show()
    personDF.select($"name").show()
    //7.过滤出DataFrame中年龄大于30的人数
    personDF.filter($"age" > 30).show()

    //8.统计DataFrame中年龄大于30的人数
    println(personDF.filter($"age" > 30).count())

    //9.统计DataFrame中按照年龄进行分组，求每个组的人数
    personDF.groupBy("age").count().show()

    //10.显示所有字段的名称
    personDF.columns.foreach(x => println(x))

    //11.把所有的age字段的结果+1
    personDF.select($"id", $"name", $"age" + 1).show()

    //-----------------------------SQL操作风格------------------------------
    //将DataFrame注册成表
    personDF.createOrReplaceTempView("t_person")

    //通过sparkSession操作sql语句

    //1.查询t_person表中的所有数据
    spark.sql("select * from t_person").show()

    //2.查询t_person表中zhangsan的信息
    spark.sql("select * from t_person where name='zhangsan'").show()

    //3.查询t_person表中的所有数据按照年龄倒叙
    spark.sql("select * from t_person order by age desc").show()

    //关闭
    sc.stop()
    spark.stop()
  }

}
