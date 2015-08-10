package com.jabong.dap.model.custorder

import com.jabong.dap.data.acq.common._
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.ad4push.variables.DevicesReactions
import com.jabong.dap.model.customer.data.CustomerDeviceMapping
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 * Created by pooja on 9/7/15.
 */
class ComponentExecutor extends Serializable with Logging {

  def start(paramJsonPath: String) = {
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      implicit val formats = net.liftweb.json.DefaultFormats
      val path = new Path(paramJsonPath)
      val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
      ParamJobConfig.paramJobInfo = json.extract[ParamJobInfo]
      ParamJsonValidator.validate(ParamJobConfig.paramJobInfo)
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
      for (paramJob <- ParamJobConfig.paramJobInfo.params) {
        ParamJobConfig.paramInfo = paramJob
        paramJob.source match {
          case DataSets.AD4PUSH => DevicesReactions.start(paramJob)
          case DataSets.CUSTOMER_DEVICE_MAPPING => CustomerDeviceMapping.start(paramJob)
          case _ => logger.error("Unknown source.")
        }
      }
    }

  }

}