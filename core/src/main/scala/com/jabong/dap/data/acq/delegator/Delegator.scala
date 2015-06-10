package com.jabong.dap.data.acq.delegator


import com.jabong.dap.common.json.parser.Parser
import com.jabong.dap.data.acq.common.{Fetcher, ImportInfo}

/**
 * Reads and parses the JSON file to run various
 * data collection jobs.
 */
class Delegator(master: String) extends Serializable {
  def start() = {
    val confFilePath = "/home/rachit/Documents/tables.json"
    val info = Parser.parseJson[ImportInfo](confFilePath)
    for(table <- info.acquisition) {
      table.source match {
          case "erp" | "bob" | "unicommerce" => new Fetcher(table).fetch()
          case _ => println("Unknown table source.")
      }
    }
  }
}
