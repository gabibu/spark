package lastfm.jobs

import lastfm.schemas.LastFM
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import utils.{SparkUtils, StringUtils}

object LongestSessionsJob {

  val JOB_NAME = "LongestSessions";

  def main(args: Array[String]): Unit = {

    val parameters = new LongestSessionParameters(args)

    val spark = SparkUtils.createSession(JOB_NAME,
      parameters.master.toOption)


    val listeningDF = spark.read
      .option(SparkUtils.DELIMITER, StringUtils.TSV_DELIMITER).
      schema(LastFM.LISTENING_HABITS_SCHEMA)
      .csv(parameters.input())


   val longestSessionsDF = new LongestSessions().getLongestSessions(listeningDF,
     parameters.maxDelataInMinutesForSession(), parameters.numberOfTopLongestSessionsToReturn())

    //todo: write to?
  }
}
