# Spark-Scala-tasks

![](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/Apache%20Spark%20Logo.png)

This Repository Demonstrates the work done by me while Learning [Apache Spark](http://spark.apache.org/). Code Snippets contains **SCALA** and **SQL**. It contains the following Code Snippets:

## Spark-Core:
* [RDD Transformations and Actions](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparkcore/RDDTransformationsActions.scala)
* [Sample WordCount Program](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparkcore/wordcount.scala)

## Spark-SQL:
* [Dataset-API](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/Dataset-API.scala)
* [Exporting MySQL Data](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/Export_MySql_data.scala)
* [MS-SQL Data Processing](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/get_MSSqlData.scala)
* [Oracle Data Processing](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/get_Oracledata.scala)
* [Redshift Data Processing](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/get_Redshiftdata.scala)
* [Joining MySQL-Oracle Data](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/join_MySql-Oracle_Data.scala)
* [Processing XML Data](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/process_XML_data.scala)
* [Processing Cassandra Data](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/process_cassandra_data.scala)
* [RDD DataFrame Tasks](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/rdd_dataframe_tasks.scala)
* [Spark_Dataframes](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/spark_dataframes.scala)
* [Spark_UDF Tasks](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/spark_udf_tasks.scala)
* [Hbase-Phoenix Data Processing](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/Hbase-pheonix_data.scala)
* [Orc-Parquet-Hive Data Processing](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/Orc-Parquet-Hive_Data.scala)
* [Fire_Service Analysis](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/Fire_Dept_Call_Service_analysis_POC.scala)
* [Real Estate Data Analysis](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/RealEstate_data_analysis.scala)
* [Airline Data Analysis](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/airlinedataanalysis_csv.scala)
* [Worldbank Data Analysis_json](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/sparksql/worldbankdatanalysis_json.scala)

You Can Learn **Scala** [Here](https://twitter.github.io/scala_school/)
You Can Learn **SQL** [Here](https://www.w3schools.com/sql/)

## What is Spark and Why should I Use it ?

Well.Apache Spark is a lightning-fast cluster computing technology, designed for fast computation. It is based on [Hadoop MapReduce](https://data-flair.training/blogs/hadoop-mapreduce-tutorial/) and it extends the MapReduce model to efficiently use it for more types of computations, which includes interactive queries and stream processing. The main feature of Spark is its in-memory cluster computing that increases the processing speed of an application.

Spark is designed to cover a wide range of workloads such as batch applications, iterative algorithms, interactive queries and streaming. Apart from supporting all these workload in a respective system, it reduces the management burden of maintaining separate tools.

## Features of Apache Spark

Apache Spark has following features.
- **Speed** − It helps to run an Application in Hadoop Cluster, upto **100 times Faster in-memory**, and **10 times faster when running   on disk**. This is possible by reducing number of read/write operations to disk. It stores intermediate processing data in memory.
- **Supports Multiple Languages** - Spark provides built-in APIs in **Java**, **Scala** and **Python**.It comes up with 80 high-level     operators for interactive querying.
- **Advanced Analytics** - Spark not only Supports **'Map'** and **'Reduce'**. It also supports **SQL Queries, Streaming Data, Machine   Learning and Graph Algorithms**

## Components of Apache Spark:
![](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/apache-spark-ecosystem-components.jpg)

## Resilient Distributed Datasets(RDD):

You Should also know about RDDs before working on Spark Environment. 
- Resilient Distributed Datasets (RDD) is a **fundamental data structure of Spark**. It is an immutable distributed collection of objects.     Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster. RDDs can contain any   type of Python, Java, or Scala objects, including user-defined classes.
- Formally, an RDD is a read-only, partitioned collection of records. RDDs can be created through deterministic operations on either       data on stable storage or other RDDs. RDD is a fault-tolerant collection of elements that can be operated on in parallel.
- There are **two ways** to create RDDs − parallelizing an existing collection in your driver program, or referencing a dataset in an     external storage system, such as a shared file system, HDFS, HBase, or any data source offering a Hadoop Input Format.

You can Learn More about Apache Spark [Here](https://data-flair.training/blogs/spark-tutorial/)

## Now Let's set up the BigData Dev Environment for Windows or Linux:

## WINDOWS(64-Bit): 
- **NOTE:** 
    - All are Direct Downloadable Links
    - Create a new folder in C:Drive as **work** and then copy it to **work** folder and add in Environment Variables(each seperately)
    - In Environment Variables Create a `HOME` for specific softwares and **add** in path below with `\bin` if required

* You Need to Install `JAVA` first which can be found [Here.](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

 Check if it is installed in your machine by using:

> java -version

If it is properly Installed You will see something like this:

```  
  java version "1.8.0_221" 
  Java(TM) SE Runtime Environment (build 1.7.0_71-b13) 
  Java HotSpot(TM) Client VM (build 25.0-b02, mixed mode)
```

Then,Install `SCALA-2.11.8` Version which is [Here](https://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.msi) and Check it using
this command:

> scala -version

You will see something like this:

```
Scala code runner version 2.11.8 -- Copyright 2002-2016, LAMP/EPFL
```

Download **SBT** which means **SCALA Build-Tool** Version 1.2.6 can be found [Here](https://piccolo.link/sbt-1.2.6.msi)

For IDE,I personally prefer **IntelliJ (Community Edition)** which can be found [Here](https://www.jetbrains.com/idea/download/download-thanks.html?platform=windows&code=IIC). You can Use whatever IDE's suits your work style.

If You want to Connect Remote Servers then Download [**PUTTY**](https://the.earth.li/~sgtatham/putty/latest/w64/putty-64bit-0.70-installer.msi).

To Connect and Transfer Files with other Servers then, Download [**WinSCP**](https://winscp.net/download/WinSCP-5.13.5-Setup.exe)

Download [**SQL WorkBench**](https://www.sql-workbench.eu/archive/Workbench-Build123.zip) to Connect and work with Different Databases. 

Download All the Following Softwares seperately and Extract it to **work** Folder:
- [Hadoop - 2.7.2.tar.gz](https://archive.apache.org/dist/hadoop/core/hadoop-2.7.2/hadoop-2.7.2.tar.gz)
- [Hadoop -2.7.2.zip](https://drive.google.com/file/d/1gaTqmmnvPUJTXnnckpEYFrRsyLFZroex/view?usp=sharing_eip&ts=5a79a2ec)
###### NOTE: 
 * copy `bin`, `etc` folders from this and paste it in hadoop-2.7.2.tar.gz after extracting
 * Open hadoop-2.7.2.tar.gz after extracting, edit and change your JAVA path in hadoop env(Windows Script) and save it 
 * Create a new folder called `tmp` in **C:** Drive and add Hive folder and copy the path
 * Open hadoop-2.7.2-->bin and type cmd:
 > => C:\work\hadoop-2.7.2\bin>winutils.exe  chmod 777  C:\tmp\hive      ///and press `Enter`

- [Spark 2.3.1_bin-Hadoop 2.7.tgz](http://mirrors.estointernet.in/apache/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz) 
###### Note: After Installing Hadoop and Spark go to the terminal/cmd type:

> hdfs namenode -format

For Checking whether spark has installed on your machine just type `spark-shell` in your cmd prompt/terminal and press `Enter`.
You will be something like this:

```
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://192.168.0.6:4040
Spark context available as 'sc' (master = local[*], app id = local-1566403376801).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/

Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_201)
Type in expressions to have them evaluated.
Type :help for more information.
```
## For LINUX / MacOS:

Go [Here](https://github.com/i-m-aravind/spark-scala-tasks/blob/master/hadoop%20spark%20ecosystem%20installation%20in%20Ubuntu.txt) and copy the code and execute it line by line properly and follow further Instructions.


## Acknowledgements:
I used spark 2.3.1 and scala 2.11.8 version which is highly stable by the time I created this repository. Dont Follow my code Grab some spark-scala projects from internet and start working on it. My Code is just my code snippets to get the hang of how spark works

## Contact:
You can mail me anytime s.aravindviews@gmail.com and I will respond within a day.We can collab and learn more
Good Luck all :thumbsup:

