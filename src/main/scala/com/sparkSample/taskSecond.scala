package com.sparkSample

import org.apache.spark.sql.{DataFrame,SQLContext, SparkSession}
import scala.language.implicitConversions

object taskSecond
{
  case class M_Schema(s_num:String,m_name:String,year:String,rating:String,runtime:String);

  def main(args: Array[String]): Unit =
  {

    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("Movie")
      .getOrCreate()

    import spark.implicits._
    val rdd = spark.sparkContext.textFile("C:\\Users\\ayushi.tripathi02\\IdeaProjects\\sparkLearning\\Dataset_movie.txt")


    //make rdd
    val mov = rdd.map(_.split(",")).filter(x=>x.length==5).map(x =>M_Schema(x(0),x(1),x(2),x(3),x(4)))
    // RDD -> DF
    val movDf = mov.toDF()
    //create view
    movDf.createOrReplaceTempView("movies")


    //1) The total number of movies
    val count_movie = spark.sql(s"select count(*) as count from movies")
    count_movie.show()

    //2) The maximum rating of movies
    val max_rating = spark.sql("select rating from movies order by rating desc limit 1")
    max_rating.show()

    //3) The number of movies that have maximum rating
    val max_rat_mov  = spark.sql("select m_name movie_name Movie Name, rating Rating  from movies where rating == (select rating from movies order by rating desc limit 1)")
    max_rat_mov.show()

    //4) The movies with ratings 1 and 2
    val rat_mov = spark.sql("select m_name movie,rating from movies where rating in ('1','2')")
    if (rat_mov.count()==0){println("No Movie Found with 1 or 2 Rating")}else{rat_mov.show()}

    //5) The list of years and number of movies released each year
    val year_movie = spark.sql("select year,count(*) Number of Movies as count from movies group by year")
    year_movie.show()

    //6) The number of movies that have a runtime of two hours
    val r_time = spark.sql("select m_name as Movie Name, runtime Runtime from movies where runtime == '7200'")
    r_time.show()

  }

}

