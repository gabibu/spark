
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import entities.ListeningRow
import lastfm.jobs.LongestSessions
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.functions.{col, concat_ws, size => array_size}
import utils.StringUtils


class LongestSessionsTests extends FlatSpec with Matchers with SparkSessionWrapper with BeforeAndAfterEach {

  import spark.implicits._

  val USERID1 = "U1"
  val USERID2 = "U2"
  val USERID3 = "U3"

  val ARTLIST_ID1 = "ARTLIST_ID1"
  val ARTLIST_ID2 = "ARTLIST_ID2"
  val ARTLIST_ID3 = "ARTLIST_ID3"

  val ARTLIST_NAME1 = "ARTLIST_NAME1"
  val ARTLIST_NAME2 = "ARTLIST_NAME2"
  val ARTLIST_NAME3 = "ARTLIST_NAME3"

  val TRACK_ID1 = "TRACK_ID1"
  val TRACK_ID2 = "TRACK_ID2"
  val TRACK_ID3 = "TRACK_ID3"

  val TRACK_NAME1 = "TRACK_NAME1"
  val TRACK_NAME2 = "TRACK_NAME2"
  val TRACK_NAME3 = "TRACK_NAME3"

  val START_TIME1 =
    new Timestamp(
      new DateTime(2021, 7, 1, 0, 0).toInstant.getMillis)
  val START_TIME2 = new Timestamp(new DateTime(2021, 7, 4, 0, 0).toInstant.getMillis)
  val START_TIME3 = new Timestamp(new DateTime(2021, 7, 10, 0, 0).toInstant.getMillis)
  val startTime1Plus5 = new Timestamp(START_TIME1.getTime + TimeUnit.MINUTES.toMillis(5))
  val startTime2Plus5 = new Timestamp(START_TIME2.getTime + TimeUnit.MINUTES.toMillis(5))
  val startTime2Plus10 = new Timestamp(START_TIME2.getTime + TimeUnit.MINUTES.toMillis(10))
  val startTime2Plus120 = new Timestamp(START_TIME2.getTime + TimeUnit.MINUTES.toMillis(120))
  val startTime3Plus5 = new Timestamp(START_TIME3.getTime + TimeUnit.MINUTES.toMillis(5))
  val startTime3Plus20 = new Timestamp(START_TIME3.getTime + TimeUnit.MINUTES.toMillis(20))
  val session1EndTime = new Timestamp(START_TIME1.getTime + TimeUnit.MINUTES.toMillis(39))
  val session2EndTime = new Timestamp(START_TIME2.getTime + TimeUnit.MINUTES.toMillis(18))
  val thirdSessionTime = new Timestamp(START_TIME2.getTime + TimeUnit.MINUTES.toMillis(120))

  it should "single user sessions" in {

    var df = Seq(

      //session1§
      ListeningRow(USERID1, START_TIME1,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID1, new Timestamp(START_TIME1.getTime + TimeUnit.MINUTES.toMillis(19)),
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID1, session1EndTime,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      //session2
      ListeningRow(USERID1, session2EndTime,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID1, new Timestamp(START_TIME2.getTime),
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      //session3
      ListeningRow(USERID1, thirdSessionTime,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3)
    ).toDF

    val sessionsDF = new LongestSessions().getLongestSessions(df,
      20, 10)

    sessionsDF.cache()

    sessionsDF.count() should be(3)

    sessionsDF.select(col("user_id")).distinct() count() should be(1)


    val firstSessionDF = sessionsDF.filter(
      col("first_timestamp").equalTo(START_TIME1)
        &&
        col("last_timestamp").equalTo(session1EndTime))


    firstSessionDF count() should be(1)

    val songsDF = firstSessionDF
      .select(concat_ws(StringUtils.TSV_DELIMITER, col("session_songs")).alias("songs_tsv"))

    songsDF.select("songs_tsv").first().getString(0) should be
    (Array(TRACK_NAME1, TRACK_NAME2, TRACK_NAME3).mkString(StringUtils.TSV_DELIMITER))


    //second session
    val secondSessionDF = sessionsDF.filter(
      col("first_timestamp").equalTo(START_TIME2)
        &&
        col("last_timestamp").equalTo(session2EndTime))


    secondSessionDF count() should be(1)

    val secondSessionSongsDF = secondSessionDF
      .select(concat_ws(StringUtils.TSV_DELIMITER, col("session_songs")).alias("songs_tsv"))

    secondSessionSongsDF.select("songs_tsv").first().getString(0) should be
    (Array(TRACK_NAME3, TRACK_NAME2).mkString(StringUtils.TSV_DELIMITER))


    //third session
    val thirdSessionDF = sessionsDF.filter(
      col("first_timestamp").equalTo(thirdSessionTime)
        &&
        col("last_timestamp").equalTo(thirdSessionTime))


    thirdSessionDF count() should be(1)

    val thirdSessionSongsDF = thirdSessionDF
      .select(concat_ws(StringUtils.TSV_DELIMITER, col("session_songs")).alias("songs_tsv"))

    thirdSessionSongsDF.select("songs_tsv").first().getString(0) should be
    (Array(TRACK_NAME3).mkString(StringUtils.TSV_DELIMITER))

  }


  it should "multiple users multiple sessions" in {

    val startTime3Plus5 = new Timestamp(START_TIME3.getTime + TimeUnit.MINUTES.toMillis(5))

    var df = Seq(

      //USER 1
      ListeningRow(USERID1, START_TIME1,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID1, startTime1Plus5,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID1, START_TIME2,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID1, startTime2Plus5,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),


      //USER 2

      ListeningRow(USERID2, START_TIME2,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID2, startTime2Plus5,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID2, startTime2Plus10,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID2, START_TIME3,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID2, startTime3Plus5,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),


    ).toDF


    val sessionsDF = new LongestSessions().getLongestSessions(df,
      20, 10)


    sessionsDF.cache()

    sessionsDF.count() should be(4)

    sessionsDF.select(col("user_id")).distinct() count() should be(2)

    val user1Sessions = sessionsDF.filter(col("user_id").equalTo(USERID1))

    user1Sessions count() should be(2)

    val firstSessionDF = user1Sessions.filter(
      col("first_timestamp").equalTo(START_TIME1)
        &&
        col("last_timestamp").equalTo(startTime1Plus5))


    firstSessionDF count() should be(1)

    val firstUserSongsDF = firstSessionDF
      .select(concat_ws(StringUtils.TSV_DELIMITER, col("session_songs")).alias("songs_tsv"))

    firstUserSongsDF.select("songs_tsv").first().getString(0) should be
    (Array(TRACK_NAME1, TRACK_NAME2).mkString(StringUtils.TSV_DELIMITER))


    //second session
    val firstUserSecondSessionDF = user1Sessions.filter(
      col("first_timestamp").equalTo(START_TIME2)
        &&
        col("last_timestamp").equalTo(startTime2Plus5))


    firstUserSecondSessionDF count() should be(1)

    val secondSessionSongsDF = firstUserSecondSessionDF
      .select(concat_ws(StringUtils.TSV_DELIMITER, col("session_songs")).alias("songs_tsv"))

    firstUserSecondSessionDF.select("songs_tsv").first().getString(0) should be
    (Array(TRACK_NAME3, TRACK_NAME1).mkString(StringUtils.TSV_DELIMITER))


  }

  it should "Take top Sessions" in {

    var df = Seq(

      //USER 1
      //user1 5 minutes
      ListeningRow(USERID1, START_TIME1,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID1, startTime1Plus5,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      //user1 10 minutes
      ListeningRow(USERID1, START_TIME2,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),


      ListeningRow(USERID1, startTime2Plus10,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      //user1 60 minutes
      ListeningRow(USERID1, START_TIME3,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID1, startTime3Plus20,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),


      //USER 2 -> 10 minutes
      ListeningRow(USERID2, START_TIME2,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID2, startTime2Plus5,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID2, startTime2Plus10,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      //user2 -> 5 minutes
      ListeningRow(USERID2, START_TIME3,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID2, startTime3Plus5,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),


    ).toDF

    val sessionsDF = new LongestSessions().getLongestSessions(df,
      20, 3)

    sessionsDF.cache()

    sessionsDF.count() should be(3)

    sessionsDF.filter(col("user_id").equalTo(USERID1)
      && col("first_timestamp").equalTo(START_TIME3)
      && col("last_timestamp").equalTo(startTime3Plus20)
      && array_size(col("session_songs")).equalTo(2)) count() should be(1)


    sessionsDF.filter(col("user_id").equalTo(USERID1)
      && col("first_timestamp").equalTo(START_TIME2)
      && col("last_timestamp").equalTo(startTime2Plus10)
      && array_size(col("session_songs")).equalTo(2)) count() should be(1)


    sessionsDF.filter(col("user_id").equalTo(USERID2)
      && col("first_timestamp").equalTo(START_TIME2)
      && col("last_timestamp").equalTo(startTime2Plus10)
      && array_size(col("session_songs")).equalTo(3)) count() should be(1)
  }
}
