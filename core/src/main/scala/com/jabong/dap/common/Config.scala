package com.jabong.dap.common

import com.jabong.dap.common.json.EmptyClass

/**
 * Case class for storing the database credentials.
 *
 * @param source String The source of the data.
 * @param driver String The driver to be used.
 * @param server String The IP of the server from where the data is to be fetched.
 * @param port String The port number to connect to.
 * @param dbName String The name of the database.
 * @param userName String The username used for logging in.
 * @param password String The password for the username
 */
case class Credentials(
  source:   String,
  driver:   String,
  server:   String,
  port:     String,
  dbName:   String,
  userName: String,
  password: String
)

/**
 * Case class for application configuration
 * here we will define application configuration
 * parameters which matches to the keys in config.json.template
 *
 * @param applicationName String Set a name for your application. Shown in the Spark web UI
 * @param master String The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
 * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
 * @param basePath String The base path for the location where the data will be saved.
 * @param credentials List[Credentials] List of credentials.
 */
case class Config(
  applicationName: String            = null,
  master:          String            = null,
  basePath:        String            = null,
  credentials:     List[Credentials] = null
) extends EmptyClass

/**
 * Object to access config variables application wide
 */
object AppConfig {
  var config: Config = null
}