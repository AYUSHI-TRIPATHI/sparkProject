package com.sparkSample

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, col, desc}


class taskFirst extends Serializable {
  def loadSurveyDf(spark:SparkSession,DataFile:String):DataFrame=
  {
    spark.read.option("header", "true")
      .option("InferSchema", "true")
      .csv("C:\\Users\\ayushi.tripathi02\\IdeaProjects\\sparkLearning\\dataset.csv").toDF()
  }

  //11.Delay
  def reasonDelay(Df11:DataFrame): Dataset[Row] = {
    Df11.groupBy("Reason").count()
      .filter(col("Reason") === "Heavy Traffic" or col("Reason") === "Weather Conditions" or col("Reason") === "Late return from Field Trip" or col("Reason") === "Delayed by School")
      .orderBy(desc("count"))
  }

  //12.Breakdown
  def reasonBreakdown(Df12:DataFrame): Dataset[Row] = {
    Df12.groupBy(col("Reason")).count()
      .filter(col("Reason") === "Problem Run" or col("Reason") === "Other" or col("Reason") === "null" or col("Reason") === "Flat Tire" or col("Reason") === "Mechanical Problem" or col("Reason") === "Won't Start" or col("Reason") === "Accident")
      .orderBy(desc(("count")))
  }


  //21.because of delay
  def route_num_delay(Df21:DataFrame): DataFrame = {
    Df21.groupBy(col("Reason"), col("Route_Number")).count()
      .orderBy(desc(("count")))
      .filter(col("Reason") === "Heavy Traffic" or col("Reason") === "Weather Conditions" or col("Reason") === "Late return from Field Trip" or col("Reason") === "Delayed by School")
      .limit(5)
      .select("Route_Number","count")
  }


  //22.because of breakdown
  def route_num_breakdown(Df22:DataFrame): DataFrame = {
    Df22.groupBy(col("Reason"), col("Route_Number")).count()
      .orderBy(desc(("count")))
      .filter(col("Reason") === "Problem Run" or col("Reason") === "Other" or col("Reason") === "null" or col("Reason") === "Flat Tire" or col("Reason") === "Mechanical Problem" or col("Reason") === "Won't Start" or col("Reason") === "Accident")
      .limit(5)
      .select("Route_Number","count")
  }


  // 31. Not In the bus
  def incidentNotinbus(Df31:DataFrame): Dataset[Row] = {
    Df31.filter(col("Number_Of_Students_On_The_Bus") === 0)
      .groupBy("School_Year").count()
      .orderBy("School_Year")
  }


  //32. In the bus
  def incidentInbus(Df32:DataFrame): Dataset[Row] = {
    Df32.filter(col("Number_Of_Students_On_The_Bus") =!= 0)
      .groupBy("School_Year").count()
      .orderBy("School_Year")
  }

  //4.less accident
  def lessAccidents(Df4:DataFrame): DataFrame = {
    Df4.filter(col("Reason") === "Accident")
      .groupBy("School_Year", "Reason").count()
      .orderBy(asc("count"))
      .limit(1)
      .select("School_Year","count")
  }


////  def main(args: Array[String]): Unit = {
////    val spark_conf = new SparkConf().setAppName("READ.CSV").setMaster("local[3]")
////
////
////    val spark = SparkSession
////      .builder()
////      .config(spark_conf)
////      .getOrCreate()
//
////    val  busDf = loadSurveyDf(spark,"dataset.csv")
//
//
//    //print schema
//    //busDf.printSchema()
//
//    // 1. Most common reasons for delay
//    val delayDf = reasonDelay(busDf)
//      delayDf.show(false)
//
//    //1.Most common reasons for breakdown
//    val breakdownDf = reasonBreakdown(busDf)
//    breakdownDf.show(false)
//
//
//   //2. Top five route numbers where the bus was  delayed
//    val route_delay = route_num_delay(busDf)
//    route_delay.show(false)
//
//    //2.Top five route numbers where the bus was broke down
//    val route_breakdown =route_num_breakdown(busDf)
//    route_breakdown.show(false)
//
//
//
//    //3. The total number of incidents, year-wise, when the students were not in bus
//   val notinbus =incidentNotinbus(busDf)
//    notinbus.show(false)
//
//    // 3. The total number of incidents, year-wise, when the students were in bus
//    val inbus =incidentInbus(busDf)
//    inbus.show(false)
//
//
//     //4. The year in which accidents were less
//    val accident =lessAccidents(busDf)
//    accident.show(false)
//
//   scala.io.StdIn.readLine()
//    spark.stop()
//
////  }

}
