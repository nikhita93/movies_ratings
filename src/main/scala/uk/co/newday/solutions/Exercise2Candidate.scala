package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import sqlContext.implicits._
import org.apache.spark.sql.functions._

object Exercise2Candidate {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
   val joined_df=ratings_df.join(movies_df,"MovieID")
    val result_avg = joined_df.groupBy(ratings_df("MovieID")).agg(max("Rating") as "Max",min("Rating") as "Min",avg("Rating") as "avg")
  }
}
