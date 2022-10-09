package org.example

import org.apache.spark
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
//import sqlContext.implicits._

object SparkReadCS {

  def main(args: Array[String]): Unit = {

    //var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample-test")
      .getOrCreate();


    val df1 = spark.read.format("csv").option("header", "true").load("E:\\dataset\\nyc_taxi.csv")
    val df2 = spark.read.format("csv").option("header", "true").load("E:/dataset/nyctaxi1.csv")
    val df3 = spark.read.format("csv").option("header", "true").load("E:/dataset/nyctaxi3.csv")

    df1.printSchema()
    println("count is:" + df1.count())
    if (df1 != null & df1.count > 0) {
      print("data is present")
//      df1.coalesce(1).write.csv("E:\\dataset\\output\\file.csv")
    } else {
      print("data is not present")
    }
    if (df2 != null & df2.count > 0) {
      print("data2 is present")
    } else {
      print("data2 is not present")
    }
    if (df3 != null & df3.count > 0) {
      print("data3 is present")
    } else {
      print("data3 is not present")
    }

  }
}