package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame

object Exercise1Candidate {

  private case class Movie (movieId:Int, title:String, genre:String)
  private case class Rating (userId:Int, movieId:Int, rating:Int, timestamp:Int)

  def execute(): (DataFrame, DataFrame) =
    {
       
     val movies_df=spark.read.format("csv").option("header",false).option("inferSchema",true).option("delimiter","::").schema("MovieID integer,Title String,Genres string").load("/Users/ankittripathy/Downloads/new_day/movies.dat")
     val ratings_df=spark.read.format("csv").option("header",false).option("delimiter","::").schema("UserID integer,MovieID integer,Rating string,Timestamp string").load("/Users/ankittripathy/Downloads/new_day/ratings.dat")
    }
}
