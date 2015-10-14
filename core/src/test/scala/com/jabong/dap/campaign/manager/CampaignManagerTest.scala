package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, TestSchema }
import com.jabong.dap.data.storage.DataSets
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
/**
 * Created by rahul for com.jabong.dap.campaign.manager on 21/7/15.
 */
class CampaignManagerTest extends FlatSpec with Serializable with SharedSparkContext {
  val jsonPath: String = JsonUtils.TEST_RESOURCES + "/campaigns/campaign_config/push_campaign_conf.json"

  val conf1 = new Configuration()
  val fileSystem = FileSystem.get(conf1)
  implicit val formats = net.liftweb.json.DefaultFormats
  val path = new Path(jsonPath)
  val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
  @transient var campaignsOutData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    campaignsOutData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/manager", "campaign_output", TestSchema.campaignOutput)
  }

  "empty json String" should "return false" in {
    val status = CampaignManager.createCampaignMaps(null)
    assert(status == false)
  }

  "Correct json String with correct campaign Mail Type" should "return true" in {
    val status = CampaignManager.createCampaignMaps(json)
    assert(CampaignManager.mailTypePriorityMap.contains(55) == true)
    assert(status == true)
  }
}
