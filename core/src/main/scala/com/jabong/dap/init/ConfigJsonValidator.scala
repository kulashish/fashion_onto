package com.jabong.dap.init

import com.jabong.dap.common.{ Credentials, Config }
import com.jabong.dap.data.storage.DataSets

/**
 * Validator for the Config JSON file.
 */
object ConfigJsonValidator {

  def validateRequiredValues(config: Config) = {
    require(config.applicationName != null && config.applicationName != "", "Application name cannot be null or empty")
    require(config.basePath != null && config.basePath != "", "Base path cannot be null or empty")
    require(config.env != null && config.env != "", "env cannot be null or empty")
  }

  def validateCredentials(credentials: Credentials) = {
    val possibleSources = Array(DataSets.BOB, DataSets.ERP, DataSets.CRM, DataSets.JDARESOURCE, DataSets.UNICOMMERCE)
    val possibleDrivers = Array(DataSets.MYSQL, DataSets.SQLSERVER)

    require(credentials.source != null && credentials.source != "", "Credential source cannot be null or empty")
    require(credentials.driver != null && credentials.driver != "", "Credential driver cannot be null or empty")
    require(credentials.server != null && credentials.server != "", "Credential server cannot be null or empty")
    require(credentials.port != null && credentials.port != "", "Credential port cannot be null or empty")
    require(credentials.dbName != null && credentials.dbName != "", "Credential db name cannot be null or empty")
    require(credentials.userName != null && credentials.userName != "", "Credential user name cannot be null or empty")
    require(credentials.password != null && credentials.password != "", "Credential password cannot be null or empty")

    require(possibleSources.contains(credentials.source), "Source '%s' not recognized. Possible values: %s".
      format(credentials.source, possibleSources.mkString(",")))
    require(possibleDrivers.contains(credentials.driver), "Driver '%s' not recognized. Possible values: %s".
      format(credentials.driver, possibleDrivers.mkString(",")))
  }

  def validate(config: Config) = {
    validateRequiredValues(config)
    if (config.credentials.nonEmpty) {
      for (cr <- config.credentials) {
        validateCredentials(cr)
      }
    }
  }
}
