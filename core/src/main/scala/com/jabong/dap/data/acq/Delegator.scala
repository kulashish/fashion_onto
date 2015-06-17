package com.jabong.dap.data.acq

import com.jabong.dap.common.json.Parser
import com.jabong.dap.data.acq.common.{ Fetcher, ImportInfo }

/**
 * Reads and parses the JSON file to run various
 * data collection jobs.
 */
class Delegator() extends Serializable {
  def start(tableJsonPath: String ) = {
    val info = Parser.parseJson[ImportInfo](tableJsonPath)

    // Validate the JSON and it's parameters.
    val validated = try {
      Validator.validate(info)
      true
    } catch {
      case e: ValidationException =>
        println("Error while validating JSON: " + e.getMessage)
        false
      case e: Exception =>
        println("Some unknown error occurred: " + e.getMessage)
        throw e
        false
    }

    // Fetch the data if validation succeeded.
    if (validated) {
      for (table <- info.acquisition) {
        table.source match {
          case "erp" | "bob" | "unicommerce" => new Fetcher(table).fetch()
          case _ => println("Unknown table source.")
        }
      }
    }
  }
}
