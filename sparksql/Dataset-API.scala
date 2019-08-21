
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, _}

object DatasetAPI {
  case class uscc(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,state:String,zip:Int,phone1:String,phone2:String,email:String,web:String)
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("DatasetAPI").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("DatasetAPI").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("DatasetAPI").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    val data = args(0)
    val tab = args(1)
    val df = spark.read.format("csv").option("inferSchema","true").option("header","true").load(data)
    //create a dataset using dataframe
    val ds = df.as[uscc]
    /*val createDDL = s"""CREATE TEMPORARY VIEW us500
     USING org.apache.phoenix.spark
     OPTIONS (
     table $tab,
     zkUrl "localhost:2181")"""
    spark.sql(createDDL)
    */
    //create a table in phoenix by defining caseclass API
    ds.write.mode(SaveMode.Overwrite).format("org.apache.phoenix.spark").option("table", tab).option("zkUrl", "localhost:2181").save()
    spark.stop()
  }
}

