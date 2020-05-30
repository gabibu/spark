package hotelbookingdemand.jobs


import org.apache.spark.sql.{SaveMode, SparkSession}
import hotelbookingdemand.jobs.bookingactions.ReservationFiltering
import org.apache.spark.sql.functions.col

object CompletedBookingFilteringJob {

  def main(args: Array[String]): Unit = {

    val conf = new Parameters(args)

    val spark = SparkSession.builder
      .appName("ReservationFiltering")
      .master("local[4]")
      .getOrCreate


    val bookingDF = spark.read.parquet(conf.input.toOption.get)
    val filteredBookingDF = new ReservationFiltering().completedBookings(bookingDF)

    filteredBookingDF.withColumn("year",
      col("arrival_date_year")).
      coalesce(conf.partitions.toOption.get)
      .write.mode(SaveMode.Append).partitionBy("year").parquet(conf.out.toOption.get)
  }

}
