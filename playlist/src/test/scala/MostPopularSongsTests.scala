import java.sql.Timestamp

import entities.ListeningRow
import lastfm.jobs.MostPopularSongs
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.apache.spark.sql.functions.{col}

class MostPopularSongsTests extends FlatSpec with Matchers with SparkSessionWrapper with BeforeAndAfterEach {

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
  val START_TIME2 = new Timestamp(new DateTime(2021, 7, 2, 0, 0).toInstant.getMillis)
  val START_TIME3 = new Timestamp(new DateTime(2021, 7, 3, 0, 0).toInstant.getMillis)
  val START_TIME4 = new Timestamp(new DateTime(2021, 7, 4, 0, 0).toInstant.getMillis)


  it should "number of songs is less than max to return" in {

    var df = Seq(


      ListeningRow(USERID1, START_TIME1,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID1, START_TIME2,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID1, START_TIME3,
        ARTLIST_ID2, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID1, START_TIME2,
        ARTLIST_ID3, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID1, START_TIME2,
        ARTLIST_ID3, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID1, START_TIME2,
        ARTLIST_ID3, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID2, START_TIME1,
        ARTLIST_ID2, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID2, START_TIME2,
        ARTLIST_ID2, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID2, START_TIME3,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID2, START_TIME2,
        ARTLIST_ID3, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID3, START_TIME2,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID3, START_TIME2,
        ARTLIST_ID3, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3),

      ListeningRow(USERID3, START_TIME2,
        ARTLIST_ID3, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME3)


    ).toDF


    val mostPopularSongsDF = new MostPopularSongs().getMostPopularSongs(df, 2)


    mostPopularSongsDF.cache()

    mostPopularSongsDF count() should be (2)


    mostPopularSongsDF.filter(
      col("artist_id").equalTo(ARTLIST_ID1)
      &&
        col("track_name").equalTo(TRACK_NAME1)
        &&
        col("num_of_played").equalTo(4)
    ) count() should be (1)


    mostPopularSongsDF.filter(
      col("artist_id").equalTo(ARTLIST_ID3)
        &&
        col("track_name").equalTo(TRACK_NAME3)
        &&
        col("num_of_played").equalTo(6)
    ) count() should be (1)


  }

  it should "return top songs" in {

    var df = Seq(

      //user1
      ListeningRow(USERID1, START_TIME1,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID1, START_TIME2,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      ListeningRow(USERID1, START_TIME3,
        ARTLIST_ID2, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      //user2

      ListeningRow(USERID2, START_TIME1,
        ARTLIST_ID2, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID2, START_TIME2,
        ARTLIST_ID2, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME2),

      ListeningRow(USERID2, START_TIME3,
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),


    ).toDF

    val mostPopularSongsDF = new MostPopularSongs().getMostPopularSongs(df, 10)

    mostPopularSongsDF.cache()

    mostPopularSongsDF count() should be (2)

    mostPopularSongsDF.filter(
      col("artist_id").equalTo(ARTLIST_ID1)
        &&
        col("track_name").equalTo(TRACK_NAME1)
        &&
        col("num_of_played").equalTo(3)
    ) count() should be (1)


    mostPopularSongsDF.filter(
      col("artist_id").equalTo(ARTLIST_ID2)
        &&
        col("track_name").equalTo(TRACK_NAME2)
        &&
        col("num_of_played").equalTo(3)
    ) count() should be (1)

  }
}
