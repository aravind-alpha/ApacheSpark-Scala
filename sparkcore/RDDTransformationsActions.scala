
package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object RDDTransformationsActions {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("RDDTransformationsActions").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("RDDTransformationsActions").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("RDDTransformationsActions").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\MonthlySales.csv"
    val drdd = sc.textFile(data)
    val head= drdd.first()
    val res = drdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(1).toInt,x(2).toInt)).reduceByKey((a,b)=>a+b).filter(x=>x._2>100).filter(x=>x._1<=12).sortBy(x=>x._1,false)
    res.collect.foreach(println)
    spark.stop()
  }
}





