package lastfm.jobs

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType


class LongestSessions {

  def getLongestSessions(listeningDF: DataFrame,
                         sameSessionDiffMinutes: Int,
                         numberOfTopSessionsToTake: Int): DataFrame = {

    val sameSessionDiffSeconds = TimeUnit.MINUTES.toSeconds(sameSessionDiffMinutes)

    val userPlaysByTimestampWindow =
      Window.partitionBy(col("user_id")).orderBy(col("timestamp"))

    var usersPlaysEnrichedDF = listeningDF.withColumn("row_number",
      row_number().over(userPlaysByTimestampWindow))


    usersPlaysEnrichedDF = usersPlaysEnrichedDF.withColumn("prev_timestamp",
      lag("timestamp", 1, null).over(userPlaysByTimestampWindow))


    usersPlaysEnrichedDF = usersPlaysEnrichedDF.withColumn("diff_from_prev_in_seconds",
      col("timestamp").cast(LongType) - col("prev_timestamp").cast(LongType))


    usersPlaysEnrichedDF = usersPlaysEnrichedDF.withColumn("temp_session",
      when(col("diff_from_prev_in_seconds").isNull,
        col("row_number"))
        .when(col("diff_from_prev_in_seconds").gt(sameSessionDiffSeconds),
          col("row_number")).otherwise(lit(null)))

    usersPlaysEnrichedDF = usersPlaysEnrichedDF.withColumn("session_id",
      coalesce(col("temp_session"),
        last(col("temp_session"), ignoreNulls = true).over(userPlaysByTimestampWindow)))


    val sessionsDF = usersPlaysEnrichedDF.groupBy("session_id", "user_id").agg(
      min(col("timestamp")).alias("first_timestamp"),
      max(col("timestamp")).alias("last_timestamp"),
      sort_array(
        collect_list(
          struct(
            col("timestamp"),
            col("track_name"))

        )).alias("session_songs")
    ).withColumn("session_songs", col("session_songs.track_name"))
      .withColumn("session_time_seconds",
        col("last_timestamp").cast(LongType) - col("first_timestamp").cast(LongType))
      .drop("session_id")

    sessionsDF.withColumn("row_number",
      row_number().over(Window.orderBy(col("session_time_seconds").desc)))
      .filter(col("row_number").leq(numberOfTopSessionsToTake))
      .drop("row_number")
  }
}
