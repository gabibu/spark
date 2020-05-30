package hotelbookingdemand.jobs.bookingactions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col}

class ReservationFiltering {

  val COMPLETED_STATUSES = List("Check-Out")
  val PAID_STATUSES = List("Check-Out", "No-Show")

  def completedBookings(df: DataFrame): DataFrame = {
    filterByStatus(df, COMPLETED_STATUSES)
  }

  def paidReservations(df: DataFrame): DataFrame = {
    filterByStatus(df, PAID_STATUSES)
  }

  private def filterByStatus(df:DataFrame, statuses: List[String]): DataFrame ={

    df.filter(col("reservation_status").isin(statuses:_*))
  }

}
