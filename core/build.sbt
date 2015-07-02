name := "Alchemy"

version := "0.1"

scalaVersion := "2.10.5"

test in assembly := {}

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.4.0" % "provided"

libraryDependencies += "net.liftweb" %% "lift-json" % "2.6"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.35"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "org.clapper" %% "grizzled-slf4j" % "1.0.1")
