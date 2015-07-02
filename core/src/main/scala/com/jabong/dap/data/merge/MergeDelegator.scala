package com.jabong.dap.data.merge

import com.jabong.dap.common.json.Parser
import com.jabong.dap.data.acq.common.{MergeJobConfig, MergeJobInfo, ImportInfo}
import com.jabong.dap.data.merge.common.Merger
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException


/**
 * Created by Abhay on 1/7/15.
 */

class MergeDelegator extends Serializable with Logging {
  def start(mergeJsonPath: String) = {
    val validated = try {
      MergeJobConfig.mergeJobInfo = Parser.parseJson[MergeJobInfo](mergeJsonPath)
      //
      // Build validator for the json for merge job
      //
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

    if (validated) {
      for (mergeJob <- MergeJobConfig.mergeJobInfo.merge) {
        MergeJobConfig.mergeInfo = mergeJob
        mergeJob.source match {
          case "erp" | "bob" | "unicommerce" => new Merger().merge()
          case _ => logger.error("Unknown table source.")
        }
      }
    }

  }
}
