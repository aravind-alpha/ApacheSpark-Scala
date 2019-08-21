
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object rdddataframe_tasks {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("rdddataframe_tasks").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("rdddataframe_tasks").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("rdddataframe_tasks").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    val data = "file:///C:\\work\\datasets\\RealEstate.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimeter","|").load(data)
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab")
    res.show(10)
    spark.stop()
  }
}

