name := "Alchemy"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.1" % "provided"