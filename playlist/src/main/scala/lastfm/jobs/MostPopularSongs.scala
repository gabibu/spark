package lastfm.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

class MostPopularSongs {

  def getMostPopularSongs(listeningDF: DataFrame, maxNumberOfSongsToReturn: Int): DataFrame = {

    val songsNumberOfPlayedDF = listeningDF
      .groupBy(col("artist_id"),
        col("track_name")).count()
      .withColumnRenamed("count", "num_of_played")

    val songNumOfPlaysWindow =
      Window.orderBy(col("num_of_played").desc)

    val topPlayedSongsDF = songsNumberOfPlayedDF.withColumn("row_number",
      row_number().over(songNumOfPlaysWindow))
      .filter(col("row_number").leq(maxNumberOfSongsToReturn))
      .drop("row_number")

    topPlayedSongsDF
  }
}
