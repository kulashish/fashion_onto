name := "Alchemy"

version := "1.0"

scalaVersion := "2.11.6"

resolvers += Resolver.sonatypeRepo("public")

resolvers += "Spark 1.4 RC4" at "https://repository.apache.org/content/repositories/orgapachespark-1112/"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0-rc4" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.0-rc4" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.4.0-rc4" % "provided"
