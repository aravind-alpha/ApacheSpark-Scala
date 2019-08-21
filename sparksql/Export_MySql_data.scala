
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExportMySql {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("ExportMySql").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("ExportMySql").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("ExportMySql").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
   //Add mysql jar in product structures
    val data = "C:\\work\\datasets\\creditcard.csv"
    val df =  spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    val url = "jdbc:oracle:thin://@oracledb.ca1vzgwad0om.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val username = "musername5"
    val password = "mpassword5"
    val drivername = "com.mysql.cj.jdbc.Driver"
    val tab = "creditcard"
    val prop = new java.util.Properties()
    prop.setProperty("user",username)
    prop.setProperty("password",password)
    prop.setProperty("driver",drivername)
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab")
    res.show()
    //export Mysql data
    res.write.jdbc(url,tab,prop)
    spark.stop()
  }
}

