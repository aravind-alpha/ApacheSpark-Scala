
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//get Oracle data and process it...finally store it in MSSql data
//get mssql data and store it in S3 path
object gerMSSqlData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("gerMSSqlData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("gerMSSqlData").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("gerMSSqlData").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    //MSSQL data
    val msurl = "jdbc:sqlserver://mssqldb.ca1vzgwad0om.ap-south-1.rds.amazonaws.com:1433;databaseName=tempdb"
    val msusername = "msusername"
    val mspassword = "mspassword"
    val drivername = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val msprop = new java.util.Properties()
    msprop.setProperty("user",msusername)
    msprop.setProperty("password",mspassword)
    msprop.setProperty("driver",drivername)

    val data = "C:\\work\\datasets\\zips.json"
    val df = spark.read.format("json").option("header","true").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select city, loc[0] lang, loc[1] latitude, pop,state, _id id from tab")
    res.show()
    //export to mssql
    res.write.jdbc(msurl,"zipdata",msprop)

    val msdf = spark.read.jdbc(msurl,"EMP",msprop)
    msdf.show()
    /*//Oracle data
    val ourl = "jdbc:oracle:thin://@oracledb.ca1vzgwad0om.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val ousername = "ousername"
    val opassword = "opassword"
    val driver = "oracle.jdbc.OracleDriver"
    val oprop = new java.util.Properties()
    oprop.setProperty("user",ousername)
    oprop.setProperty("password",opassword)
    oprop.setProperty("driver",driver)*/
    spark.stop()
  }
}

