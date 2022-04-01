package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import sqlContext.implicits._
import org.apache.spark.sql.functions._

object Exercise3Candidate {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
    val result_rating = joined_df.withColumn("rank",row_number().over(Window.partitionBy("UserID").orderBy($"Rating".desc))).where($"Rank"<='3').show()
  }
}
