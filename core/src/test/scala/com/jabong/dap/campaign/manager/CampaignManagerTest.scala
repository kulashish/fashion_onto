package com.jabong.dap.campaign.manager

import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.scalatest.FlatSpec

/**
 * Created by rahul for com.jabong.dap.campaign.manager on 21/7/15.
 */
class CampaignManagerTest extends FlatSpec with Serializable {
  val jsonPath: String = "src/test/resources/campaign/campaign_config/push_campaign_conf.json"
  val conf = new Configuration()
  val fileSystem = FileSystem.get(conf)
  implicit val formats = net.liftweb.json.DefaultFormats
  val path = new Path(jsonPath)
  val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString) //"empty config json file"

  "empty json String" should "return false" in {
    val status = CampaignManager.createCampaignMaps(null)
    assert(status == false)
  }

  "Correct json String" should "return true" in {
    val status = CampaignManager.createCampaignMaps(json)
    assert(CampaignManager.campaignPriorityMap.contains("cancelReTarget") == true)
    assert(status == true)
  }

  "Correct json String with wrong campaign Name" should "return false" in {
    val status = CampaignManager.createCampaignMaps(json)
    assert(CampaignManager.campaignPriorityMap.contains("cancelReTarge") != true)
    assert(status == true)
  }
}
