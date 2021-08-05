//
//
//
//import org.scalatest.{FlatSpec, Matchers}
//import org.scalatest.BeforeAndAfterEach
//
//import entities.Booking
//import hotelbookingdemand.jobs.bookingactions.ReservationFiltering
//
//import org.apache.spark.sql.functions.{col, cume_dis}
//class FilteringTests  extends FlatSpec with Matchers with SparkSessionWrapper with BeforeAndAfterEach {
//  spark.sparkContext.setLogLevel("ERROR")
//  import spark.implicits._
//
//
//
//  case class Salary(depName: String, empNo: Long, salary: Long)
//
//
//
//
//  it should "filtering completed bookings" in {
//
//    val bookingDF = Seq(Booking("h1", true, 1, 2, "jan", 1, 1, 1,1,1,0,0,null,
//      "is", "ms", "dc", 1, 1, 1, "r1", "r1", 1, "No Deposit",
//      1, 1, 0, "ct", 1f, 0, 0, "Check-Out", "s"),
//      Booking("h2", true, 1, 2, "jan", 1, 1, 1,1,1,0,0,null,
//        "is", "ms", "dc", 1, 1, 1, "r1", "r1", 1, "No Deposit",
//        1, 1, 0, "ct", 1f, 0, 0, "No-Show", "s"),
//      Booking("h3", true, 1, 2, "jan", 1, 1, 1,1,1,0,0,null,
//        "is", "ms", "dc", 1, 1, 1, "r1", "r1", 1, "No Deposit",
//        1, 1, 0, "ct", 1f, 0, 0, "Check-Out", "s")).toDF
//
//    bookingDF.withColumn("cc", mei(col("ds")))
//
//    val completedBookingDF = new ReservationFiltering().completedBookings(bookingDF)
//
//    completedBookingDF.cache()
//
//    completedBookingDF count() should be(2)
//
//    completedBookingDF.filter(col("hotel").equalTo("h1")) count() should be(1)
//    completedBookingDF.filter(col("hotel").equalTo("h3")) count() should be(1)
//
//  }
//
//
//
//
//  override def beforeEach() {
//    println("cleaning cache")
//    spark.catalog.clearCache()
//  }
//}
