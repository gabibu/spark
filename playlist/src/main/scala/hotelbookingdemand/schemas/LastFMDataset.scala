package hotelbookingdemand.schemas

import org.apache.spark.sql.types._

object LastFMDataset {


  val LISTENING_HABITS_SCHEMA = new StructType()
    .add("user_id", StringType)
    .add("timestamp", TimestampType)
    .add("artist_id", StringType)
    .add("artist_name", StringType)
    .add("track_id", StringType)
    .add("track_name", StringType)

}
