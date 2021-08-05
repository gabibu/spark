import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, Suite}

trait SparkSessionWrapper extends Suite with BeforeAndAfterEach {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .config("spark.master", "local")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def beforeEach() {
    println("cleaning cache")
    spark.catalog.clearCache()
  }
}
