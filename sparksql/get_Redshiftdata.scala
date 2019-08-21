
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object getRedshiftdata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("getRedshiftdata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("getRedshiftdata").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("getRedshiftdata").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    val url = "jdbc:redshift://aravind.cmorjhhxlgfn.ap-south-1.redshift.amazonaws.com:5439/reddb"
    val prop = new java.util.Properties()
    prop.setProperty("user","rusername")
    prop.setProperty("password","Rpassword.1")
    prop.setProperty("driver","com.amazon.redshift.jdbc.Driver")
    val data = "C:\\work\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    val res = spark.read.jdbc(url,"EMP",prop)
    res.show()
    df.write.jdbc(url,"usdata",prop)
    spark.stop()
  }
}

