
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RealEstatedata_analysis {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("RealEstatedata_analysis").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("RealEstatedata_analysis").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("RealEstatedata_analysis").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    //small analysis of real estate data using spark
    //TASK - with "realestate.csv" do => group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.
    val data = "file:///C:\\work\\datasets\\RealEstate.csv"
    //as it is "," seperated data change its delimeter to "|".So create a dataframe
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimeter","|").load(data)
    //as the column "Price SQ Ft" has space inbetween, remove it to avoid exceptions.We can also use this regex
    val df1 = df.withColumnRenamed("Price SQ Ft","pricesqft")
    //instead of using SQL queries,Use "dsl commands"
    df1.select("location","price".trim,"pricesqft".trim).distinct.show(10)
    spark.stop()
  }
}

