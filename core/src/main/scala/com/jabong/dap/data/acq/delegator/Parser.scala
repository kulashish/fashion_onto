package com.jabong.dap.data.acq.delegator

import net.liftweb.json._

import scala.io.Source

case class TableInfo(source: String, name: String, primaryKey: String, mode: String, incrementType: String,
                      mergeType: String, dateColumn: String, rangeStart: String, rangeEnd: String)

case class ImportInfo(tables: List[TableInfo])

/**
 * Reads and parses the JSON file to run various
 * data collection jobs.
 */
class Parser(master: String) extends Serializable {
  def start() = {
    val confFilePath = "/home/rachit/projects/Alchemy/src/main/scala/com/jabong/conf/tables.json"
    implicit val formats = DefaultFormats

    val source = Source.fromFile(confFilePath)
    val lines = try source.getLines.mkString("\n") finally source.close()

    val json = parse(lines)
    val info = json.extract[ImportInfo]
    for(table <- info.tables) {
      table.source match {
        case "erp" => println("erp")
      }
    }
  }
}
