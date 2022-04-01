package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame

object Exercise4Candidate {

  def execute(movies: DataFrame, ratings: DataFrame, movieRatings:DataFrame, ratingWithRankTop3:DataFrame) = {
  joined_df.write.format("parquet").save("joined_tbl1.parquet")
  result_rating.write.format("parquet").save("result_rating.parquet")
  result_avg.write.format("parquet").save("result_avg.parquet")

  }
}
