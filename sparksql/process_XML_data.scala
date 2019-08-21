
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object processXMLdata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("processXMLdata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("processXMLdata").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("processXMLdata").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    val data = "C:\\work\\datasets\\processxml.xml"
    val url = "jdbc:oracle:thin://@oracledb.ca1vzgwad0om.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df = spark.read.format("xml").option("rowTag","course").load(data)
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select crse, days, instructor, place.building placebuilding, place.room placeroom, sect,subj, reg_num, time.end_time endtime, time.start_time starttime, units from tab")
    res.show()
    res.write.jdbc(url,"xmldata",prop)
    spark.stop()
  }
}

