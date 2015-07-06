package com.jabong.dap.data.storage.merge

import com.jabong.dap.data.acq.common._
import com.jabong.dap.data.storage.merge.common.Merger
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }

/**
 * Created by Abhay on 1/7/15.
 */

class MergeDelegator extends Serializable with Logging {
  def start(mergeJsonPath: String) = {
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      implicit val formats = net.liftweb.json.DefaultFormats
      val path = new Path(mergeJsonPath)
      val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
      MergeJobConfig.mergeJobInfo = json.extract[MergeJobInfo]
      MergeJsonValidator.validate(MergeJobConfig.mergeJobInfo)
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
