package com.abyss.spark.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by Abyss on 2018/8/28.
  * description:
  */
//定义一个样例类：Student
case class People(id: Int, name: String, age: Int)

//利用sparkSql将数据写入到mysql
object SparkSqlToMysql {
  def main(args: Array[String]): Unit = {

    //1.创建sparSession对象
    val spark = SparkSession.builder()
      .appName("SparkToMysql")
      .master("local[2]")
      .enableHiveSupport() //开启对hive的支持
      .getOrCreate()

    //2.获取sparkContext对象
    val sc = spark.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")

    //3.通过sparkContext读取数据文件
    val dataRDD = sc.textFile("./data/people.txt")

    //4.切分每一行
    val lineArrayRDD = dataRDD.map(_.split(","))

    //5.可以将lineArrayRDD和样例类Person向关联
    val peopleRDD = lineArrayRDD.map(x => People(x(0).toInt, x(1), x(2).toInt))

    //6.将personRDD转化为DataFrame
    //需要手动导入隐式转换的包
    import spark.implicits._
    val peopleDF = peopleRDD.toDF()

    //7.将DataFrame注册成表
    peopleDF.createOrReplaceTempView("t_student")
    //打印dataFrame中的结果数据
    peopleDF.show()

    //8.查询t_person表中的所有数据按照年龄倒序，返回DataFrame
    val resultDF = spark.sql("select * from t_student order by age desc")

    //9.将resultDF写入到mysql表中
    //准备配置属性
    val properties = new Properties()
    //设置用户名和密码
    properties.setProperty("user","root")
    properties.setProperty("password","root")

    //如果该表不存在会自动创建
    //mode需要对应4个参数
    //overwrite:覆盖（它会帮你创建一个表，然后进行覆盖）
    //append:追加（它会帮你创建一个表，然后把数据追加到表里面）
    //ignore:忽略（它表示只要当前表存在，它就不会进行任何操作）
    //ErrorIfExists:只要表存在就报错（默认选项）
    resultDF.write.mode("overwrite").jdbc("jdbc:mysql://node1:3306/spark","student",properties)
    //打成jar包时的提交脚本
    /*
    * spark-submit --master spark://node1:7077 --class com.abyss.spark.sql.SparkSqlToMysql --executor-memory 1g --
    * total-executor-cores 2 --jars /export/server/hive/lib/mysql-connector-java-5.1.35.jar --driver-class-path
    * /export/server/hive/lib/mysql-connector-java-5.1.35.jar original-spark-2.0.jar /people.txt aa
    * */

    //关闭
    sc.stop()
    spark.stop()

  }
}
