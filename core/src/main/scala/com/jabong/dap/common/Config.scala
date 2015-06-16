package com.jabong.dap.common

import com.jabong.dap.common.json.EmptyClass

/**
 * Case class for application configuration
 * here we will define application configuration
 * parameters which matches to the keys in config.json.template
 *
 * @param applicationName String Set a name for your application. Shown in the Spark web UI
 * @param master String The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
 * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
 */
case class Config(
  var applicationName: String = null,
  var master: String = null
) extends EmptyClass

/**
 * Object to access config variables application wide
 */
object AppConfig extends Config {
  var config: Config = null
}