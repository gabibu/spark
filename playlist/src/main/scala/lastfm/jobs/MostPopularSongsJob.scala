package lastfm.jobs

import lastfm.jobs.UserNumberOfDistinctSongsJob.JOB_NAME
import lastfm.schemas.LastFM
import utils.{SparkUtils, StringUtils}

class MostPopularSongsJob {

  def main(args: Array[String]): Unit = {

    val parameters = new MostPopularSongsParameters(args)
    val spark = SparkUtils.createSession(JOB_NAME,
      parameters.master.toOption)

    val listeningDF = spark.read
      .option(SparkUtils.DELIMITER, StringUtils.TSV_DELIMITER).
      schema(LastFM.LISTENING_HABITS_SCHEMA)
      .csv(parameters.input())


    val mostPopularSongsPlayedDF =
      new MostPopularSongs().getMostPopularSongs(listeningDF, parameters.maxNumberOfSongsToReturn())

    //todo: write to?
  }
}
