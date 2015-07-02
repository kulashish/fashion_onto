package com.jabong.dap.data.acq

import com.jabong.dap.common.json.Parser
import com.jabong.dap.data.acq.common._
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException

/**
 * Reads and parses the JSON file to run various
 * data collection jobs.
 */
class Delegator extends Serializable with Logging {



  def start(tableJsonPath: String) = {
    val validated = try {
      AcqImportInfo.importInfo = Parser.parseJson[ImportInfo](tableJsonPath)
      TablesJsonValidator.validate(AcqImportInfo.importInfo)
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
      for (table <- AcqImportInfo.importInfo.acquisition) {
        AcqImportInfo.tableInfo = table
        table.source match {
          case "erp" | "bob" | "unicommerce" => new Fetcher().fetch(table)
          case _ => logger.error("Unknown table source.")
        }
      }
    }
  }
}
