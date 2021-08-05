package hotelbookingdemand.jobs


import org.apache.spark.sql.functions.{col, row_number}
import hotelbookingdemand.SparkUtils
import hotelbookingdemand.schemas.LastFMDataset
import org.apache.spark.sql.expressions.Window


object MostPopularSongs {


  def main(args: Array[String]): Unit = {

    val conf = new Parameters(args)

    val spark = SparkUtils.createSession("ss",
      Option("local[4]"))


    val usersPlayedSongsDF = spark.read.option("delimiter", "\t").
      schema(LastFMDataset.LISTENING_HABITS_SCHEMA)
      .csv(conf.input.toOption.get)


    val songsNumberOfPlayedDF = usersPlayedSongsDF
      .groupBy(col("artist_id"),
      col("track_name")).count()
        .withColumnRenamed("count", "song_num_of_played")


    val songNumOfPlaysWindow =
      Window.orderBy(col("song_num_of_played"))


    var topPlayedSongsDF = songsNumberOfPlayedDF.withColumn("row_number",
      row_number().over(songNumOfPlaysWindow))
      .filter(col("row_number").leq(100))
        .drop("row_number")


    topPlayedSongsDF
  }

}
