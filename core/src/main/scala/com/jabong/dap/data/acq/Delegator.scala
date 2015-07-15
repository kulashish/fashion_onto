package com.jabong.dap.data.acq

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.data.acq.common._
import com.jabong.dap.data.acq.history.getHistoricalData
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }

/**
 * Reads and parses the JSON file to run various
 * data collection jobs.
 */
class Delegator extends Serializable with Logging {

  def start(tableJsonPath: String) = {
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      implicit val formats = net.liftweb.json.DefaultFormats
      val tablesPath = new Path(tableJsonPath)
      val json = parse(scala.io.Source.fromInputStream(fileSystem.open(tablesPath)).mkString)
      AcqImportInfo.importInfo = json.extract[ImportInfo]
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
      if (OptionUtils.getOptBoolVal(AcqImportInfo.importInfo.isHistory)) {
        for (table <- AcqImportInfo.importInfo.acquisition) {
          AcqImportInfo.tableInfo = table
          table.source match {
            case "erp" | "bob" | "unicommerce" => new getHistoricalData().fetchData(table)
            case _ => logger.error("Unknown table source.")
          }
        }
      } else {
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
}
