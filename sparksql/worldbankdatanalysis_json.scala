
package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object process_jsondata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("process_jsondata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("process_jsondata").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("process_jsondata").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    val input = "C:\\work\\datasets\\world_bank.json"
    //val input = args(0)
    //val output = args(1)
    val df = spark.read.format("json").option("inferSchema","true").load(input)
    df.createOrReplaceTempView("tab")
    val res= spark.sql("select  distinct _id.`$oid` as id, theme1.Name themename, theme1.Percent themepercent,cast(tn.code as Int) themenamecode,sn.code sncode,sector4.Name s4name,sector3.Name s3name,sector2.percent s2percent,s.Name sname,pd.DocType pddoctype, mjtnc.name mjtncname,mjt,mjsnc.code mjsnccode,mjsp.Name mjspname, tn.name themenamecodename, url, themecode from tab lateral view explode(theme_namecode) t as tn lateral view explode(sector_namecode) t as sn lateral view explode(sector) t as s lateral view explode(projectdocs) t as pd lateral view explode(mjtheme_namecode) t as mjtnc lateral view explode(mjtheme) t as mjt lateral view explode(mjsector_namecode) t as mjsnc lateral view explode(majorsector_percent) t as mjsp")
    df.printSchema()
    res.printSchema()
    res.show(10)
    //res.write.format("csv").option("header","true").save(output)
    spark.stop()
  }
}

