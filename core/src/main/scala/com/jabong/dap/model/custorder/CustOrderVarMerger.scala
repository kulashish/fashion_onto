package com.jabong.dap.model.custorder

import com.jabong.dap.data.acq.common._
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.ad4push.variables.DevicesReactions
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 * Created by pooja on 9/7/15.
 */
class CustOrderVarMerger extends Serializable with Logging {

  def start(COVarJsonPath: String) = {
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      implicit val formats = net.liftweb.json.DefaultFormats
      val path = new Path(COVarJsonPath)
      val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
      COVarJobConfig.coVarJobInfo = json.extract[COVarJobInfo]
      COVarJsonValidator.validate(COVarJobConfig.coVarJobInfo)
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
      for (coVarJob <- COVarJobConfig.coVarJobInfo.coVar) {
        COVarJobConfig.coVarInfo = coVarJob
        coVarJob.source match {
          case DataSets.AD4PUSH => DevicesReactions.customerResponse(coVarJob.date, coVarJob.mode)
          case _ => logger.error("Unknown source.")
        }
      }
    }

  }

}
