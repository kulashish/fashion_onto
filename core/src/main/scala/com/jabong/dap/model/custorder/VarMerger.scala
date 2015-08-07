package com.jabong.dap.model.custorder

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.data.acq.common._
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.ad4push.variables.DevicesReactions
import com.jabong.dap.model.customer.data.CustomerDeviceMapping
import com.jabong.dap.model.product.itr.BasicITR
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 * Created by pooja on 9/7/15.
 */
class VarMerger extends Serializable with Logging {

  def start(VarJsonPath: String) = {
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      implicit val formats = net.liftweb.json.DefaultFormats
      val path = new Path(VarJsonPath)
      val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
      VarJobConfig.varJobInfo = json.extract[VarJobInfo]
      VarJsonValidator.validate(VarJobConfig.varJobInfo)
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
      val isHistory = OptionUtils.getOptBoolVal(VarJobConfig.varJobInfo.isHistory)
      for (varJob <- VarJobConfig.varJobInfo.vars) {
        VarJobConfig.varInfo = varJob
        varJob.source match {
          case DataSets.AD4PUSH => DevicesReactions.start(varJob)
          case DataSets.CUSTOMER_DEVICE_MAPPING => CustomerDeviceMapping.start(varJob)
          case DataSets.BASIC_ITR => BasicITR.start(varJob, isHistory)
          case _ => logger.error("Unknown source.")
        }
      }
    }

  }

}
