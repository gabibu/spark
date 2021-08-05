import java.sql.Timestamp

import entities.xx
import hotelbookingdemand.jobs.LongestSessions
import org.scalatest.FlatSpec
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.BeforeAndAfterEach




class LongestSessionsTests extends FlatSpec with Matchers with SparkSessionWrapper with BeforeAndAfterEach {

  import spark.implicits._


  val USERID_1  = "U1"
  val USERID_2  = "U2"
  val USERID_3  = "U3"

  val ARTLIST_ID1  = "ARTLIST_ID1"
  val ARTLIST_ID2  = "ARTLIST_ID2"
  val ARTLIST_ID3  = "ARTLIST_ID3"

  val ARTLIST_NAME1  = "ARTLIST_NAME1"
  val ARTLIST_NAME2  = "ARTLIST_NAME2"
  val ARTLIST_NAME3  = "ARTLIST_NAME3"

  val TRACK_ID1  = "TRACK_ID1"
  val TRACK_ID2  = "TRACK_ID2"
  val TRACK_ID3  = "TRACK_ID3"

  val TRACK_NAME1  = "TRACK_NAME1"
  val TRACK_NAME2  = "TRACK_NAME2"
  val TRACK_NAME3  = "TRACK_NAME3"

  it should "filtering completed bookings" in {

    var df =  Seq(

      xx(USERID_1, new Timestamp(2021,
      7, 1, 0, 0, 0, 0 ),
      ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
      TRACK_NAME1),

      xx(USERID_1, new Timestamp(2021,
        7, 1, 0, 19, 0, 0 ),
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      xx(USERID_1, new Timestamp(2021,
        7, 1, 0, 39, 0, 0 ),
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),


      xx(USERID_1, new Timestamp(2021,
        7, 3, 0, 0, 0, 0 ),
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1),

      xx(USERID_1, new Timestamp(2021,
        7, 3, 0, 15, 0, 0 ),
        ARTLIST_ID1, ARTLIST_NAME1, TRACK_ID1,
        TRACK_NAME1)
    ).toDF


    new LongestSessions().getLongestSessions(df,
      20)

  }
}
