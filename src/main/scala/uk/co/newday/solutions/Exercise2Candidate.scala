package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame

object Exercise2Candidate {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
   val joined_df=ratings_df.join(movies_df,"MovieID")
    val result_avg = joined_df.groupBy(ratings_df("MovieID")).agg(max("Rating") as "Max",min("Rating") as "Min",avg("Rating") as "avg")
  }
}
