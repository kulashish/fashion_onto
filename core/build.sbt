name := "Alchemy"

version := "0.1.7"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

libraryDependencies += "com.sun.mail" % "javax.mail" % "1.5.3"

libraryDependencies += "javax.mail" % "javax.mail-api" % "1.5.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.2.1"  % "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-launcher_2.10" % "1.4.1" % "provided"

libraryDependencies += "net.liftweb" %% "lift-json" % "2.6"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.35"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.0.3"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "org.clapper" %% "grizzled-slf4j" % "1.0.1",
  "org.pegdown" % "pegdown" % "1.0.2")

unmanagedJars in Compile += file("lib/sqljdbc4.jar")

test in assembly := {}
assemblyJarName in assembly := "Alchemy-assembly.jar"

(testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/report")
(testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-oT")


//seq(lsSettings :_*)
