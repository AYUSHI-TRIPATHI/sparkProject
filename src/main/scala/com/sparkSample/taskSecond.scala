package com.sparkSample

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, col, desc}


class taskSecond extends Serializable {
  def loadSurveyDf(spark:SparkSession,DataFile:String):DataFrame=
  {
    spark.read.option("header", "true")
      .option("InferSchema", "true")
      .csv("C:\\Users\\ayushi.tripathi02\\IdeaProjects\\sparkLearning\\dataset.csv").toDF()
  }

  // 1. Most common reasons for delay
  def reasonDelay(Df11:DataFrame): Dataset[Row] = {
    Df11.groupBy("Reason").count()
      .filter(col("Reason") === "Heavy Traffic" or col("Reason") === "Weather Conditions" or col("Reason") === "Late return from Field Trip" or col("Reason") === "Delayed by School")
      .orderBy(desc("count"))
  }

  //1.Most common reasons for breakdown
  def reasonBreakdown(Df12:DataFrame): Dataset[Row] = {
    Df12.groupBy(col("Reason")).count()
      .filter(col("Reason") === "Problem Run" or col("Reason") === "Other" or col("Reason") === "null" or col("Reason") === "Flat Tire" or col("Reason") === "Mechanical Problem" or col("Reason") === "Won't Start" or col("Reason") === "Accident")
      .orderBy(desc(("count")))
  }

  //2. Top five route numbers where the bus was  delayed
  def route_num_delay(Df21:DataFrame): DataFrame = {
    Df21.groupBy(col("Reason"), col("Route_Number")).count()
      .orderBy(desc(("count")))
      .filter(col("Reason") === "Heavy Traffic" or col("Reason") === "Weather Conditions" or col("Reason") === "Late return from Field Trip" or col("Reason") === "Delayed by School")
      .limit(5)
      .select("Route_Number","count")
  }

  //2.Top five route numbers where the bus was broke down
  def route_num_breakdown(Df22:DataFrame): DataFrame = {
    Df22.groupBy(col("Reason"), col("Route_Number")).count()
      .orderBy(desc(("count")))
      .filter(col("Reason") === "Problem Run" or col("Reason") === "Other" or col("Reason") === "null" or col("Reason") === "Flat Tire" or col("Reason") === "Mechanical Problem" or col("Reason") === "Won't Start" or col("Reason") === "Accident")
      .limit(5)
      .select("Route_Number","count")
  }

  //3. The total number of incidents, year-wise, when the students were not in bus
  def incidentNotinbus(Df31:DataFrame): Dataset[Row] = {
    Df31.filter(col("Number_Of_Students_On_The_Bus") === 0)
      .groupBy("School_Year").count()
      .orderBy("School_Year")
  }

  // 3. The total number of incidents, year-wise, when the students were in bus
  def incidentInbus(Df32:DataFrame): Dataset[Row] = {
    Df32.filter(col("Number_Of_Students_On_The_Bus") =!= 0)
      .groupBy("School_Year").count()
      .orderBy("School_Year")
  }

  //4. The year in which accidents were less
  def lessAccidents(Df4:DataFrame): DataFrame = {
    Df4.filter(col("Reason") === "Accident")
      .groupBy("School_Year", "Reason").count()
      .orderBy(asc("count"))
      .limit(1)
      .select("School_Year","count")
  }
}
