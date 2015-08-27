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
  source: String,
  driver: String,
  server: String,
  port: String,
  dbName: String,
  userName: String,
  password: String)

/**
 * Case class for application configuration
 * here we will define application configuration
 * parameters which matches to the keys in config.json.template
 *
 * @param applicationName String Set a name for your application. Shown in the Spark web UI
 * @param readOutputPath String The output path for the location where the data will be read from.
 *                       The data here is the putput of some previous job.
 * @param writeOutputPath String The output path for the location where the data will be saved.
 * @param basePath String The base path for the location where the data will be read and written.
 * @param credentials List[Credentials] List of credentials.
 */
case class Config(
  applicationName: String = null,
  basePath: String = null,
  readOutputPath: Option[String] = null,
  writeOutputPath: Option[String] = null,
  credentials: List[Credentials] = null) extends EmptyClass

/**
 * Object to access config variables application wide
 */
object AppConfig {
  var config: Config = null
}