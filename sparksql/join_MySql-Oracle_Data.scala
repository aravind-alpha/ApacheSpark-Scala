
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object joinMySqlOracleData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("joinMySqlOracleData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("joinMySqlOracleData").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("joinMySqlOracleData").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    val output = "C:\\work\\datasets\\output\\joinmysqloracle"
    //Oracle Data
    val odata = "C:\\work\\datasets\\2008.csv"
    val ouser = "ousername"
    val opass = "opassword"
    val ourl =  "jdbc:oracle:thin://@oracledb.ca1vzgwad0om.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val drivername = "oracle.jdbc.OracleDriver"
    val oprop = new java.util.Properties()
    oprop.setProperty("user",ouser)
    oprop.setProperty("password",opass)
    oprop.setProperty("driver",drivername)
    val odf = spark.read.format("csv").option("header","true").load(odata)
    //MySqlData
    val mdata = "C:\\work\\datasets\\2007.csv"
    val muser = "musername"
    val mpass = "mpassword"
    val murl =  "jdbc:mysql://mysqldb.ca1vzgwad0om.ap-south-1.rds.amazonaws.com:3306/mysqldb"
    val driver = "com.mysql.cj.jdbc.Driver"
    val mprop = new java.util.Properties()
    mprop.setProperty("user",muser)
    mprop.setProperty("password",mpass)
    mprop.setProperty("driver",driver)
    val mdf = spark.read.format("csv").option("header","true").load(mdata)
    mdf.createOrReplaceTempView("mtab")
    odf.createOrReplaceTempView("otab")
    val join = spark.sql("select m.distance,m.origin,o.Flightnum from otab o join mtab m on m.FlightNum=o.FlightNum")
    join.show(10)
    //join.write.format("csv").option("header","true").save(output)
    //if you want to overwrite the data then use
    //if you want only one partition rdd then use coalesce
    //join.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(output)
    //after 1 year there will be no rdds*/
    spark.stop()
  }
}

