name := "sparkJobs"

version := "0.1"

scalaVersion := "2.12.7"



libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "2.4.0" % Test

libraryDependencies += "org.rogach" %% "scallop" % "3.4.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % Test