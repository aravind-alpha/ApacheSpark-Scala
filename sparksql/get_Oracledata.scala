package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object getOracledata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("getOracledata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("getOracledata").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("getOracledata").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    //add oracle jar in project modules
    //csv data
    /*val url = "jdbc:oracle:thin://@oracledb.ca1vzgwad0om.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val query = "(SELECT e.ename, e.job, e.sal, d.loc, d.deptno FROM EMP e join DEPT d on e.deptno=d.deptno where sal > 2800) tmp)"
    val tab = "EMP"
    val username = "ousername"
    val password = "opassword"
    val drivername = "oracle.jdbc.OracleDriver"
    val output = "C:\\work\\datasets\\results"
    val prop = new java.util.Properties()
    prop.setProperty("user",username)
    prop.setProperty("password",password)
    prop.setProperty("driver",drivername)
    val df = spark.read.jdbc(url,tab,prop)
    df.show()*/
    //json data
    val data = "C:\\work\\datasets\\zips.json"
    val tab = "zipdata"
    val df = spark.read.format("json").option("inferSchema","true").option("header","true").load(data)
    val url = "jdbc:oracle:thin://@oracledb.ca1vzgwad0om.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val username = "ousername"
    val password = "opassword"
    val drivername = "oracle.jdbc.OracleDriver"
    val prop = new java.util.Properties()
    prop.setProperty("user",username)
    prop.setProperty("password",password)
    prop.setProperty("driver",drivername)
   //if you want to process million records then, use this prop
    prop.setProperty("partitionColumn","Flightnum")
    prop.setProperty("lowerBound","1")
    prop.setProperty("upperBound","100")
    prop.setProperty("numPartitions","10")
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select city, loc[0] lang, loc[1] lati, pop, _id id from tab")
    res.show()
    //export to oracle
    res.write.jdbc(url,tab,prop)
    spark.stop()
  }
}

