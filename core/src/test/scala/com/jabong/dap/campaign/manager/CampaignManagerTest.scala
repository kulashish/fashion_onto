package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ Spark, SharedSparkContext }
import com.jabong.dap.data.storage.schema.Schema
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FlatSpec

/**
 * Created by rahul for com.jabong.dap.campaign.manager on 21/7/15.
 */
class CampaignManagerTest extends FlatSpec with Serializable with SharedSparkContext {
  val jsonPath: String = "src/test/resources/campaign/campaign_config/push_campaign_conf.json"
  val conf1 = new Configuration()
  val fileSystem = FileSystem.get(conf1)
  implicit val formats = net.liftweb.json.DefaultFormats
  val path = new Path(jsonPath)
  val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
  @transient var campaignsOutData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    campaignsOutData = JsonUtils.readFromJson("campaign/manager", "campaign_output", Schema.campaignOutput)
  }

  "empty json String" should "return false" in {
    val status = CampaignManager.createCampaignMaps(null)
    assert(status == false)
  }

  "Correct json String" should "return true" in {
    val status = CampaignManager.createCampaignMaps(json)
    assert(CampaignManager.campaignPriorityMap.contains(CampaignCommon.CANCEL_RETARGET_CAMPAIGN) == true)
    assert(status == true)
  }

  "Correct json String with wrong campaign Name" should "return false" in {
    val status = CampaignManager.createCampaignMaps(json)
    assert(CampaignManager.campaignPriorityMap.contains("cancelReTarge") != true)
    assert(status == true)
  }

  "Correct json String with correct campaign Mail Type" should "return true" in {
    val status = CampaignManager.createCampaignMaps(json)
    assert(CampaignManager.mailTypePriorityMap.contains(55) == true)
    assert(status == true)
  }

  "No Input Campaigns Data" should "return null" in {
    val status = CampaignManager.createCampaignMaps(json)
    val expectedData = CampaignManager.campaignMerger(null)
    assert(expectedData == null)
  }

  "Input Campaigns Data but no priority map loaded" should "return null" in {
    CampaignManager.mailTypePriorityMap.clear()
    val mergedCampaignData = CampaignManager.campaignMerger(campaignsOutData)
    assert(mergedCampaignData == null)
  }

  "Input Campaigns Data with priority map loaded" should "return two " in {
    val status = CampaignManager.createCampaignMaps(json)
    val mergedCampaignData = CampaignManager.campaignMerger(campaignsOutData)
    assert(mergedCampaignData.count() == 2)
  }

  "Test add Priority" should "add one more column" in {
    val status = CampaignManager.createCampaignMaps(json)
    val mergedCampaignData = CampaignUtils.addPriority(campaignsOutData)
    mergedCampaignData.show(5)
    assert(mergedCampaignData.columns.length == 4)
  }

}
