package hotelbookingdemand.jobs.bookingactions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{struct, col, when, avg, min}
import org.apache.spark.sql.expressions.Window

class StructureBooingAction {

  def completedBookings(df: DataFrame): DataFrame = {


    val hotelPartitionWindow = Window.partitionBy("hotel")
    val hotelCustomerTypePartitionWindow = Window.partitionBy("hotel", "customer_type")


    var structuredDataDF = df.withColumn("customer", struct(col("previous_cancellations"),
      col("is_repeated_guest"),
      col("previous_bookings_not_canceled"),
      col("customer_type")))

    structuredDataDF = structuredDataDF.withColumn("hotel_avg_of_special_requests", avg(col("hotel_avg_of_special_requests").over(hotelPartitionWindow)))
    structuredDataDF = structuredDataDF.withColumn("hotel_customer_type_avg_of_special_requests", avg(col("hotel_avg_of_special_requests").over(hotelCustomerTypePartitionWindow)))

    structuredDataDF = structuredDataDF.withColumn("hotel_min_arrival_date_year", min(col("arrival_date_year").over(hotelPartitionWindow)))

    structuredDataDF = structuredDataDF.withColumn("deposit_type",
      when(col("deposit_type").equalTo("No Deposit"), "NoDeposit")
        .when(col("deposit_type").equalTo("Refundable"),
          "depositWithRefund").when(col("deposit_type").equalTo("Non Refund"),
        "depositWithoutRefund").otherwise(null))


    structuredDataDF = structuredDataDF.withColumn("lead",struct(col("lead_time"),
      col("distribution_channel"),
      col("market_segment"), col("agent"),
      col("company")))

    structuredDataDF = structuredDataDF.withColumn("reservation",
      struct(col("stays_in_weekend_nights"),
      col("stays_in_week_nights"),
      col("adults"),
      col("children"),
      col("babies"),
      col("meal"),
      col("reserved_room_type"),
      col("deposit_type"),
      col("days_in_waiting_list"),
      col("required_car_parking_spaces"),
      col("total_of_special_requests"),
      col("reservation_status"),
      col("reservation_status_date"),
      col("Hotel").alias("hotel"),
      col("is_canceled"),
      col("booking_changes"),
      col("arrival_date_year"),
      col("arrival_date_month"),
      col("arrival_date_week_number"),
      col("arrival_date_day_of_month"),
      col("assigned_room_type"),
      col("adr").alias("alternative_dispute_resolution")
    ))

    return structuredDataDF.select("reservation", "lead", "customer")
  }
}
