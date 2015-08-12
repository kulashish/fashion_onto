package com.jabong.dap.campaign.manager


import com.jabong.dap.campaign.campaignlist._
import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.campaign.{CampaignMergedFields, CampaignCommon}
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.{ CampaignConfig, CampaignInfo }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by Mubarak on 7/8/15.
 */
class CampaignProcessorTest extends FlatSpec with Serializable with SharedSparkContext {

    val jsonPath: String = "src/test/resources/campaign/campaign_config/push_campaign_conf.json"
    val conf1 = new Configuration()
    val fileSystem = FileSystem.get(conf1)
    implicit val formats = net.liftweb.json.DefaultFormats
    val path = new Path(jsonPath)
    val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
    @transient var campaignsData: DataFrame = _
    @transient var cmr: DataFrame = _
    @transient var res1: DataFrame = _
    @transient var itr: DataFrame = _
    @transient var res2: DataFrame = _

    override def beforeAll() {
    super.beforeAll()
    campaignsData = JsonUtils.readFromJson("campaign/processor", "campaignInput", Schema.campaignPriorityOutput)
    cmr = JsonUtils.readFromJson("extras", "res1")
    itr = JsonUtils.readFromJson("campaign/processor", "itr")
    val status = CampaignManager.createCampaignMaps(json)
    }

    "Test mapDeviceFromCMR" should "return 6" in {
      val res = CampaignProcessor.mapDeviceFromCMR(cmr, campaignsData)
      res1 = res
      assert(res.count() == 6)
    }

  "Test campaignMerger" should "return 6" in {
    val res = CampaignProcessor.campaignMerger(res1, CampaignMergedFields.CUSTOMER_ID, CampaignMergedFields.DEVICE_ID)
    res2 = res
    assert(res.count() == 3)
  }

  "Test mergeCampaigns" should "return 2" in {
    val res = CampaignProcessor.mergepushCampaigns(res1, itr)
    println("mergeCampaigns:")
    res.collect().foreach(println)
    assert(res.count() == 2)
  }


}