package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame

object Exercise3Candidate {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
    val result_rating = joined_df.withColumn("rank",row_number().over(Window.partitionBy("UserID").orderBy($"Rating".desc))).where($"Rank"<='3').show()
  }
}
