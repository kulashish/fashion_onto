package com.jabong.dap.data.acq

import com.jabong.dap.common.json.Parser
import com.jabong.dap.data.acq.common.{ Fetcher, ImportInfo }
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException

/**
 * Reads and parses the JSON file to run various
 * data collection jobs.
 */
class Delegator extends Serializable with Logging {
  def start(tableJsonPath: String) = {
    var info: ImportInfo = null
    // Parse and validate the JSON and it's parameters.
    val validated = try {
      info = Parser.parseJson[ImportInfo](tableJsonPath)
      TablesJsonValidator.validate(info)
      true
    } catch {
      case e: ParseException =>
        logger.error("Error while parsing JSON: " + e.getMessage)
        false

      case e: IllegalArgumentException =>
        logger.error("Error while validating JSON: " + e.getMessage)
        false

      case e: Exception =>
        logger.error("Some unknown error occurred: " + e.getMessage)
        throw e
        false
    }

    // Fetch the data if validation succeeded.
    if (validated) {
      for (table <- info.acquisition) {
        table.source match {
          case "erp" | "bob" | "unicommerce" => new Fetcher().fetch(table)
          case _ => logger.error("Unknown table source.")
        }
      }
    }
  }
}
