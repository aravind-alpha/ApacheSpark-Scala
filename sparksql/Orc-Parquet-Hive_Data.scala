
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Orc_Parquet_Hive_Data {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Orc_Parquet_Hive_Data").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Orc_Parquet_Hive_Data").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Orc_Parquet_Hive_Data").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    //processing orc/parquet/hive and storing in hive or EMR cluster/Remote server
    //val data = "C:\\work\\datasets\\zips.json"
    val data = args(0)
    val orcop = args(1)
    val csvop = args(2)
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    df.show()
    val res = spark.sql("select city, pop, state, _id id, loc[0] lang, loc[1] lati from tab")
    res.write.format("orc").saveAsTable("ziporc")
    res.write.format("parquet").saveAsTable("zipparq")
    res.write.format("orc").save(orcop)
    res.write.format("csv").option("header","true").option("inferSchema","true").save(csvop)
    println("completed successfully")
    //If you want to read orc/parquet data..follow as same as the above way
    val orcpath = args(0)
    val parqpath = args(1)
    val orcdf = spark.read.format("orc").load(orcpath)
    val parqdf = spark.read.format("parquet").load(parqpath)
    orcdf.createOrReplaceTempView("orc")
    parqdf.createOrReplaceTempView("parq")
    val result = spark.sql("select p.cty, p.pop, o.id, o.lang, o.lati from orc o join parq p on o.id=p.id")
    result.show()
    //if you want to read two tables directly from hive then,
    val tab1 = args(0)
    val tab2 = args(1)
    val df1 = spark.sql(s"select * from $tab1")
    val df2 = spark.sql(s"select * from $tab2")
    val results = spark.sql("select p.cty, p.pop, o.id, o.lang, o.lati from ziporc zo join zipparq zp on o.id=p.id")
    df1.show(5)
    df2.show(5)
    results.show(5)
    spark.stop()
  }
}

