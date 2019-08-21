
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object csv_data_process {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("csv_data_process").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("csv_data_process").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("csv_data_process").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    //Analysing most travelled flights using RDD
    val data = "file:///C:\\work\\datasets\\2008.csv"
    val drdd = sc.textFile(data)
    //if you want to skip the header //create a structure to the data or cleaning the data
    val head = drdd.first()
    val process = drdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(10),1)).reduceByKey(_+_).sortBy(x=>x._2,false).toDF("Flight","Travelled")
    //run sql queries
    process.createOrReplaceTempView("Airline")
    val res = spark.sql("select * from Airline")
    //show only 5 results
    res.show(10)
    spark.stop()
  }
}

