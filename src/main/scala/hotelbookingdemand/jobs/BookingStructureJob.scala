package hotelbookingdemand.jobs

object BookingStructureJob extends BaseJob {

  def main(args: Array[String]): Unit = {

    val conf = new Parameters(args)

    val spark = createSession("BookingStructureJob", "local[4]")
    val bookingDF = spark.read.parquet(conf.input.toOption.get)
  }
}
