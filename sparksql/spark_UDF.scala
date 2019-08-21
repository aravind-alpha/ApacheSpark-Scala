
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparkUDF {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkUDF").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkUDF").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkUDF").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val data = "C:\\work\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("tab")
    /*//concat_ws....combining 2 columns as 1 column
    //regexp_extract....extracting a particular record
    val res0 = spark.sql("select first_name, city, state, zip, email, regexp_extract(email, 'gmail.com',0)gmailholders from tab")
    //regexp_replace...replacing certain elements
    val res1 = spark.sql("select first_name, city, state, email, regexp_replace(phone1, '-','') phone from tab")
    val res2 = spark.sql("select first_name, city, state, zip, email, regexp_replace(email, 'gmail','googlemail')gmailholders from tab")
    //if you want to add a new column and specify for example "nstate" then,
    val ndf = df.drop("phone2","county","zip","address","web")
    val res = ndf.withColumn("nstate", when($"state"==="NY","New York").otherwise($"state"))
    res.show()*/
    //create a function
    val off1 = (x:String) => x match {
    //def off1 (x:String) = x match {
    case "NY" => "20% off"
      case "NJ" => "30% off"
      case "MI" => "40% off"
      case _ => "10% off"
    }
     //copy the functions to udf
     val off = udf(off1)
     //val offer = udf(off1 _)
    //if you want to run sql queries then
    //val offerdf = spark.sql("select *, offer(state) offers from tab")
     val offerdf = df.withColumn("offers", off($"state")).drop("zip","phone1","phone2","country","email","web")
     offerdf.show()
    spark.stop()
  }
}

