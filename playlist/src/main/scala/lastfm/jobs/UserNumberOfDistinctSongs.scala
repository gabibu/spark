package lastfm.jobs

import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions.{countDistinct, col}

class UserNumberOfDistinctSongs {


  def getUsersNumberOfDistinctSongsPlayed(listeningDF: DataFrame): DataFrame = {

    listeningDF.groupBy(col("user_id"))
      .agg(countDistinct(col("track_name")).alias("number_of_distinct_song_played"))
  }

}
