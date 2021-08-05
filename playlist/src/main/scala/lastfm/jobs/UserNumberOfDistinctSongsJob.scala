package lastfm.jobs

import lastfm.schemas.LastFM
import utils.{SparkUtils, StringUtils}

object UserNumberOfDistinctSongsJob {

  val JOB_NAME = "UserNumberOfDistinctSongsJob";

  def main(args: Array[String]): Unit = {

    val parameters = new UserNumberOfDistinctSongsParameters(args)

    val spark = SparkUtils.createSession(JOB_NAME,
      parameters.master.toOption)


    val listeningDF = spark.read
      .option(SparkUtils.DELIMITER, StringUtils.TSV_DELIMITER).
      schema(LastFM.LISTENING_HABITS_SCHEMA)
      .csv(parameters.input())


    val usersNumberOfDistinctSongsPlayedDF =
      new UserNumberOfDistinctSongs()
        .getUsersNumberOfDistinctSongsPlayed(listeningDF)

    //todo: write to?
  }
}
