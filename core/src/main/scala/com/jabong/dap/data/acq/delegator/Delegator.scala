package com.jabong.dap.data.acq.delegator


import com.jabong.dap.common.json.parser.{EmptyClass, Parser}
import com.jabong.dap.data.acq.common.Fetcher

case class JoinTables(name: String, foreignKey: String)

case class TableInfo(source: String, name: String, primaryKey: String, mode: String, saveFormat: String,
                      dateColumn: String, rangeStart: String, rangeEnd: String, limit: String,
                      joinTables: List[JoinTables])

case class ImportInfo(acquisition: List[TableInfo]) extends EmptyClass

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
          case "erp" => new Fetcher(master).fetch()
          case _ => println("unknown")
      }
    }
  }
}
