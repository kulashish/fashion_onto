package com.jabong.dap.init

import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.common.{ AppConfig, Config, Spark }
import com.jabong.dap.data.acq.Delegator
import com.jabong.dap.data.storage.merge.MergeDelegator
import com.jabong.dap.model.clickstream.variables.{ GetSurfVariables, SurfVariablesMain }
import com.jabong.dap.model.custorder.ComponentExecutor
import com.jabong.dap.model.product.itr.Itr
import com.jabong.dap.quality.campaign.MobilePushCampaignQuality
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import scopt.OptionParser

object Init {

  /**
   * Define command line option parameters
   *
   * @param component String Name of the component
   * @param tableJson String Path of data acquisition config json file
   * @param mergeJson String Path of merge job config json file
   * @param config String Path of application config json file
   */
  case class Params(
    component: String = null,
    tableJson: String = null,
    mergeJson: String = null,
    paramJson: String = null,
    pushCampaignsJson: String = null,
    config: String = null)

  def main(args: Array[String]) {
    options(args)
  }

  /**
   * Check for command line options
   * kick action based upon action
   * action passed.
   * @param args Array[String]
   */
  def options(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Alchemy") {
      opt[String]("component")
        .text("Component name like 'itr/acquisition/erp/campaign' etc.")
        .required()
        .action((x, c) => c.copy(component = x))

      opt[String]("mergeJson")
        .text("Path to merge job json config file.")
        .action((x, c) => c.copy(mergeJson = x))

      opt[String]("tablesJson")
        .text("Path to data acquisition tables json config file.")
        .action((x, c) => c.copy(tableJson = x))

      opt[String]("config")
        .text("Path to Alchemy config file.")
        .required()
        .action((x, c) => c.copy(config = x))

      opt[String]("paramJson")
        .text("Path to customer and Order variables merge job json config file.")
        .action((x, c) => c.copy(paramJson = x))

      opt[String]("pushCampaignsJson")
        .text("Path to push Campaigns priority config file.")
        .action((x, c) => c.copy(pushCampaignsJson = x))

    }

    parser.parse(args, defaultParams).map { params =>
      // read application file
      try {
        val conf = new Configuration()
        val fileSystem = FileSystem.get(conf)
        implicit val formats = net.liftweb.json.DefaultFormats
        val path = new Path(params.config)
        val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
        val config = json.extract[Config]

        ConfigJsonValidator.validate(config)
        AppConfig.config = config
        // initialize spark context
        Spark.init(new SparkConf().setAppName(AppConfig.config.applicationName))
        run(params)
      } catch {
        case e: ParseException =>
          println("Error while parsing JSON: " + e.getMessage)
          throw e

        case e: IllegalArgumentException =>
          println("Error while validating JSON: " + e.getMessage)
          throw e

        case e: Exception =>
          println("Some unknown error occurred: " + e.getMessage)
          throw e
      }
    }.getOrElse {
      sys.exit(1)
    }
  }

  /**
   * Trigger action based upon the component passed
   * @param params
   */
  def run(params: Params): Unit = {
    params.component match {
      case "itr" => new Itr().start()
      case "basicItr" => new ComponentExecutor().start(params.paramJson)
      case "acquisition" => new Delegator().start(params.tableJson) // do your stuff here
      case "merge" => new MergeDelegator().start(params.mergeJson)
      case "deviceMapping" => new ComponentExecutor().start(params.paramJson)
      case "Ad4pushCustReact" => new ComponentExecutor().start(params.paramJson)
      case "pushRetargetCampaign" => CampaignManager.startPushRetargetCampaign()
      case "pushInvalidCampaign" => CampaignManager.startPushInvalidCampaign(params.pushCampaignsJson)
      case "pushAbandonedCartCampaign" => CampaignManager.startPushAbandonedCartCampaign(params.pushCampaignsJson)
      case "pushWishlistCampaign" => CampaignManager.startWishlistCampaigns(params.pushCampaignsJson)
      case "pushCampaignMerge" => CampaignManager.startPushCampaignMerge(params.pushCampaignsJson)
      case "pushSurfCampaign" => CampaignManager.startSurfCampaigns(params.pushCampaignsJson)

      // clickstream use cases
      case "clickstreamYesterdaySession" => SurfVariablesMain.startClickstreamYesterdaySessionVariables()
      case "clickstreamSurf3Variable" => SurfVariablesMain.startSurf3Variable()
      case "clickstreamSurf3MergeData30" => GetSurfVariables.getSurf3mergedForLast30Days()

      //campaign quality check
      case "mobilePushCampaignQuality" => MobilePushCampaignQuality.startMobilePushCampaignQuality(params.pushCampaignsJson)

      //pricing sku data
      case "pricingSKUData" => new ComponentExecutor().start(params.paramJson)

        // dcf feed
      case "dcfFeedGenerate" => new ComponentExecutor().start(params.paramJson)
    }
  }
}
