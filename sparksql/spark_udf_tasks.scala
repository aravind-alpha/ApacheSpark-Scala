
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object udftasks {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("udftasks").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("udftasks").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("udftasks").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    //oracle data process by spark UDF
    val url = "jdbc:oracle:thin://@oracledb.ca1vzgwad0om.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df = spark.read.jdbc(url,"EMP",prop)
    //if you want to replace null values to 0 in commission column then use,
    import org.apache.spark.sql.functions.{udf, when}
    //val fullsal= df.withColumn("val",when($"comm".isNull, 0).otherwise($"comm")).withColumn("fullsal",$"sal"+$"val").drop("sal","comm","val")
    //fullsal.show()
    val fullsalary = (x:Int) => x match {
      //def off1 (x:String) = x match {
      case 5000 => "20% off"
      case 3000 => "30% off"
      case 1600 => "40% off"
      case _ => "10% off"
    }
    //copy the functions to udf
    val sal = udf(fullsalary)
    val fullsal = df.withColumn("val",when($"comm".isNull, 0).otherwise($"comm")).withColumn("fullsal",$"sal"+$"val").drop("sal","comm","val")
    fullsal.show()
    spark.stop()
  }
}

