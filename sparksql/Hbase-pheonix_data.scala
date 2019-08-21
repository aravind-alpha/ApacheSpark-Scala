
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object pheonix_data {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("pheonix_data").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("pheonix_data").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("pheonix_data").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    //submitting in production environment
    val tab = args(0)
    val op = args(1)
    val df = spark.read.format("org.apache.phoenix.spark").option("table", "tab").option("zkUrl", "localhost:2181").load()
    df.show()
    df.write.format("csv").option("header","true").save(op)
    spark.stop()
  }
}

