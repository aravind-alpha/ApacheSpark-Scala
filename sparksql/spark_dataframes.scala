
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object dataframes {
//2 case class nepcc(name:String, email:String, mobile:Int)
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("dataframes").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("dataframes").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("dataframes").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    //val data = "file:///C:\\work\\datasets\\nep.csv"
    //val data = args(0)
    //val nerdd = sc.textFile(data)
    //val heading = nerdd.first()
    /*val process = nerdd.filter(x=>x!=heading).map(x=>x.split(",")).map(x=>(x(0).replaceAll(" ",""),x(1),x(2))).toDF("names","email","phone")
   //run sql queries
   process.createOrReplaceTempView("tab")
   process.printSchema()
   val res = spark.sql("select * from tab")
   res.show()*/

    //second way to create DF
    /*val process = nerdd.filter(x=>x!=heading).map(x=>x.split(",")).map(x=>nepcc(x(0),x(1),x(2).toInt)).toDF()
    process.show()*/

    //third way to create DF
    /*import org.apache.spark.sql.types._
    //schema
    val cols = "name,email,mobile"
    val fields = cols.split(",").map(x =>StructField(x, StringType, nullable = true))
    val schema = StructType(fields)
    //data
    // Convert records of the RDD (people) to Rows
    val strdata = nerdd.filter(x=>x!=heading).map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2)))
    // Apply the schema to the RDD
    val df = spark.createDataFrame(strdata, schema)
    df.show()
    df.printSchema()*/

    //4th way the best way to use in Production environment //Dataframe API
    val input = "C:\\work\\datasets\\ca-500.csv"
    //val input = args(0)
    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input)
    df.show()
    df.printSchema()
    df.createOrReplaceTempView("tab")
    val res = spark.sql("SELECT CONCAT(c.first_name, ' ', c.last_name) AS full_name, c.* FROM  `tab` c")
    res.show()
    spark.stop()
  }
}

