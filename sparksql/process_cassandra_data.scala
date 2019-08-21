package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, _}

object cassandradata {

  def main(args: Array[String]) {

    //val spark = SparkSession.builder.master("local[*]").appName("cassandradata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()

    val spark = SparkSession.builder.master("local[*]").appName("cassandradata").config("spark.cassandra.connection.host","localhost").getOrCreate()

    val sc = spark.sparkContext

    val conf = new SparkConf().setAppName("cassandradata").setMaster("local[*]")

    // val sc = new SparkContext(conf)

    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val sqlContext = spark.sqlContext
    // val data ="file:///Users/bhuvana/work/datasets/inc1.csv"
    // val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    // val cdf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","yogi").option("table","asl").load()
    // cdf.show()
    // df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("keyspace","yogi").option("table","asl").save()

    // val data ="file:///Users/bhuvana/work/datasets/asl.txt"
    //val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

    //df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","cass").option("keyspace","yogi").save()

    val cdf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","yogi").option("table","asl").load()
    val cassdf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","yogi").option("table","cass").load()
    cdf.createOrReplaceTempView("asl")
    cassdf.createOrReplaceTempView("cass")
    val res = spark.sql("select a.id, c.age, c.name,a.city from asl a join cass c on a.name=c.name")
    res.show()
    // spark.sql("create table result (id int primary key, age int, name varchar, city varchar")

    res.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","result").option("keyspace","yogi").save()

    spark.stop()
    //  export asl data to cassandra.
    //first step create a table in cassandra, than only write data in cassandra.
  }
}

