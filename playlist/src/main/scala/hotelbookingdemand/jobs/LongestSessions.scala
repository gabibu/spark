package hotelbookingdemand.jobs

import org.apache.spark.sql.DataFrame
import hotelbookingdemand.SparkUtils
import hotelbookingdemand.schemas.LastFMDataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, datediff, lag, row_number,
  when, lit, coalesce, first}
import org.apache.spark.sql.types.LongType


class LongestSessions {



  def getLongestSessions(usersPlayedSongsDF1: DataFrame,
                         sameSessionDiffMinutes: Int): DataFrame = {



    val songNumOfPlaysWindow =
      Window.partitionBy(col("user_id")).orderBy(col("timestamp"))


    var usersPlayedSongsDF = usersPlayedSongsDF1

    usersPlayedSongsDF =  usersPlayedSongsDF.withColumn("row_number",
      row_number().over(songNumOfPlaysWindow))


    val songNumOfPlaysWindow1 =
      Window.partitionBy(col("user_id")).orderBy(col("timestamp"))



    usersPlayedSongsDF = usersPlayedSongsDF.withColumn("prev_timestamp",
      lag("timestamp", 1, null).over(songNumOfPlaysWindow1))


    usersPlayedSongsDF = usersPlayedSongsDF.withColumn("prev_row_number",
      lag("row_number", 1, null).over(songNumOfPlaysWindow1))


    usersPlayedSongsDF = usersPlayedSongsDF.withColumn("diff_from_prev_in_seconds",
      col("timestamp").cast(LongType) -
        col("prev_timestamp").cast(LongType))


    val sameSessionDiffSeconda = sameSessionDiffMinutes * 60

    usersPlayedSongsDF = usersPlayedSongsDF.withColumn("assigned_group1",
      when(col("diff_from_prev_in_seconds").isNull,
        col("row_number"))
        .when(col("diff_from_prev_in_seconds").gt(sameSessionDiffSeconda),
          col("row_number")).otherwise(lit(null)))


    usersPlayedSongsDF.withColumn("assigned_group",
      first(col("assigned_group1"), true).over(songNumOfPlaysWindow) )



    usersPlayedSongsDF

  }
}
