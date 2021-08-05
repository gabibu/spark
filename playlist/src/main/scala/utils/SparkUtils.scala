package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

    val DELIMITER = "delimiter"

   def createSession(appName : String, master : Option[String]):SparkSession ={
    var sessionBuilder = SparkSession.builder
      .appName(appName)

    if(!master.isEmpty){
      sessionBuilder.master(master.get)
    }

    sessionBuilder.getOrCreate
  }


}
