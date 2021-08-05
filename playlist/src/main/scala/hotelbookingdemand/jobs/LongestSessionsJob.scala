package hotelbookingdemand.jobs

import hotelbookingdemand.SparkUtils
import hotelbookingdemand.schemas.LastFMDataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, datediff, lag, row_number,
when, lit, coalesce, first}
import org.apache.spark.sql.types.LongType

object LongestSessionsJob {

  def main(args: Array[String]): Unit = {

    val sameSessionDiffMinutes = 20


    val conf = new Parameters(args)

    val spark = SparkUtils.createSession("ss",
      Option("local[4]"))


    var usersPlayedSongsDF = spark.read.option("delimiter", "\t").
      schema(LastFMDataset.LISTENING_HABITS_SCHEMA)
      .csv(conf.input.toOption.get)


    val songNumOfPlaysWindow =
      Window.partitionBy(col("user_id")).orderBy(col("timestamp"))


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


    usersPlayedSongsDF = usersPlayedSongsDF.withColumn("assigned_group1",
      when(col("diff_from_prev_in_seconds").isNull,
        col("row_number"))
        .when(col("diff_from_prev_in_seconds").gt(100),
        col("row_number")).otherwise(lit(null)))


    usersPlayedSongsDF.withColumn("assigned_group",
      first(col("assigned_group1"), true).over(songNumOfPlaysWindow) )


    println(1)








  }

}
