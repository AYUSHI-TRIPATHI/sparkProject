package com.sparkSample

import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.sql.SparkSession
import scala.language.implicitConversions

object taskFirstRdd extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark_conf = new SparkConf().setAppName("READ.CSV").setMaster("local[3]")
    val spark = SparkSession
      .builder()
      .config(spark_conf)
      .getOrCreate()

    //create rdd
    val rddFromCsv = spark.sparkContext.textFile(("dataset.csv"))
    //    val dataRDD = spark.read.csv("path").rdd

    //printing number of records
    println(rddFromCsv.count())
    //print only 10 rec
    rddFromCsv.take(10).foreach(println)
    //remove header
    val header = rddFromCsv.first()
    val rdd = rddFromCsv.filter(_ != header)
    //print top 10 without header
    rdd.take(10).foreach(println)

    //Most common reasons for delay
  }
}
