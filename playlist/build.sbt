name := "sparkJobs"

version := "0.1"

scalaVersion := "2.12.7"



libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "2.4.0"

libraryDependencies += "org.rogach" %% "scallop" % "3.4.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"


libraryDependencies ++= Seq(
  "org.scala-js" %% "scalajs-test-interface" % "0.6.14",
  "org.scalatest" %% "scalatest" % "3.0.1", //version changed as these the only versions supported by 2.12
  "com.novocode" % "junit-interface" % "0.11",
  "org.scala-lang" % "scala-library" % scalaVersion.value
)