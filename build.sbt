name := "BigDataMaker"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"


//test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

scalacOptions += "-target:jvm-1.8"

resolvers += Resolver.sonatypeRepo("public")

parallelExecution in Test := false

test in assembly := {}

