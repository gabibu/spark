package hotelbookingdemand.jobs

import org.apache.spark.sql.SparkSession

class BaseJob {

  protected def createSession(appName : String, master : String):SparkSession ={
    SparkSession.builder
      .appName(appName)
      .master(master)
      .getOrCreate
  }


}
