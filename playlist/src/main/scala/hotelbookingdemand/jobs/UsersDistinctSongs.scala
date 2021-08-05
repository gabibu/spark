package hotelbookingdemand.jobs

import hotelbookingdemand.SparkUtils
import hotelbookingdemand.schemas.LastFMDataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{collect_set}

object UsersDistinctSongs {


  def main(args: Array[String]): Unit = {

    val conf = new Parameters(args)

    val spark = SparkUtils.createSession("ss",
      Option("local[4]"))


    val usersPlayedSongsDF = spark.read.option("delimiter", "\t").
      schema(LastFMDataset.LISTENING_HABITS_SCHEMA)
      .csv(conf.input.toOption.get)


    usersPlayedSongsDF.groupBy(col("user_id"))
      .agg(collect_set(col("track_name"))
        .alias("user_unique_songs"))
      .repartition(conf.partitions.toOption.get)
      .write.parquet(conf.out.toOption.get)


  }
}
