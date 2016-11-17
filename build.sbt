name := "BigDataMaker"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

//test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3" % "test"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

resolvers += Resolver.sonatypeRepo("public")

parallelExecution in Test := false

test in assembly := {}

