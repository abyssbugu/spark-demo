package com.abyss.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Abyss on 2018/8/26.
  * 思路
  * 1、加载日志数据，获取ip信息
  * 2、加载ip段信息，获取ip起始和结束数字，经度,纬度
  * 3、将日志的ip分割出来，转换为数字，和ip段比较
  * 4、比较的时候采用二分法查找
  * 5、过滤出相应的值
  */
object IPLocation {

  def ip2Long(ip: String): Long = {
    //将IP地址装换成long   192.168.200.150  这里的.必须使用特殊切分方式[.]
    val ips = ip.split("\\.")
    var ipNum: Long = 0L
    for (i <- ips) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(ipNum: Long, broadcastValue: Array[(String, String, String, String)]): Int = {
    //口诀:上下循环寻上下，左移右移寻中间
    //开始下标
    var start = 0
    //结束下标
    var end = broadcastValue.length - 1

    while (start <= end) {

      var middle = (start + end) / 2
      if (ipNum >= broadcastValue(middle)._1.toLong && ipNum <= broadcastValue(middle)._2.toLong) {
        return middle
      }
      if (ipNum < broadcastValue(middle)._1.toLong) {
        end = middle
      }
      if (ipNum > broadcastValue(middle)._2.toLong) {
        start = middle
      }
    }
    -1


  }

  def data2mysql(iter: Iterator[((String, String), Int)]): Unit = {
    //定义数据库连接
    var conn:Connection=null
    //定义PreparedStatement
    var ps:PreparedStatement=null
    //编写sql语句,?表示占位符
    val sql="insert into iplocation(longitude,latitude,total_count) values(?,?,?)"
    //获取数据库连接
    conn=DriverManager.getConnection("jdbc:mysql://node1:3306/spark","root","root")
    ps=conn.prepareStatement(sql)

    //遍历迭代器
    iter.foreach(line=>{
      //对sql语句中的?占位符赋值
      ps.setString(1,line._1._1)
      ps.setString(2,line._1._2)
      ps.setInt(3,line._2)
      //执行sql语句
      ps.execute()
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ip").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //1. 读取基站城市经纬度信息
    val city_ip_rdd: RDD[(String, String, String, String)] = sc.textFile("/Users/abyss/Dev/Demos/spark-wordcount/ip.txt")
      //切分每一行数据，获取ipStart,ipEnd，经度，维度
      .map(_.split("\\|")).map(x => (x(2), x(3), x(13), x(14)))
    //2. 将城市ip信息广播到每一个worker节点
    val cityIpBroadcast = sc.broadcast(city_ip_rdd.collect())

    //3. 获取日志数据,获取所有ip地址
    val ips: RDD[String] = sc.textFile("/Users/abyss/Dev/Demos/spark-wordcount/20090121000132.394251.http.format").map(_.split("\\|")(1))
    //4. 将ip地址转化成long，然后通过二分查询去基站数据中匹配，获取到对应的经度和纬度
    val result: RDD[((String, String), Int)] = ips.mapPartitions(iter => {
      //获取广播变量中的值
      val broadcastValue: Array[(String, String, String, String)] = cityIpBroadcast.value
      iter.map(ip => {
        //将IP地址转化为Long
        val ipNum: Long = ip2Long(ip)
        //通过二分查询，找到对应的ipNum在基站数据中的下标位置
        val index: Int = binarySearch(ipNum, broadcastValue)
        val tuple = broadcastValue(index)
        //返回数据，封装成元组((经度，维度),1)
        ((tuple._3, tuple._4), 1)

      })
    })
    //5、把相同经度和维度出现的次数累加
    val finalResult = result.reduceByKey(_ + _)
    //6. 结果进行打印
    finalResult.collect().foreach(println(_))
    //7.保存结果数据到mysql表中
    finalResult.foreachPartition(data2mysql)

    //8、关闭sc
    sc.stop()

  }

}
