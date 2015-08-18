package com.jabong.dap.campaign.traceablility

import java.text.SimpleDateFormat

import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ Spark, SharedSparkContext }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FlatSpec

/**
 * Created by rahul for com.jabong.dap.campaign.traceablility on 21/7/15.
 */
class PastCampaignCheckTest extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var pastCampaignData: DataFrame = _
  @transient var customerSelected: DataFrame = _
  var pastCampaignCheck: PastCampaignCheck = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    pastCampaignCheck = new PastCampaignCheck()
    pastCampaignData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/traceability", "merged_campaign")
    customerSelected = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/traceability", "customer_selected")
  }

  "No past campaign Data" should "return no customer whom we have sent the campaign" in {
    val mailTypeCustomers = pastCampaignCheck.getCampaignCustomers(null, 46, 20)
    assert(mailTypeCustomers == null)
  }

  "No mail Type " should "return no customer whom we have sent the campaign" in {
    val mailTypeCustomers = pastCampaignCheck.getCampaignCustomers(pastCampaignData, 0, 20)
    assert(mailTypeCustomers == null)
  }

  "Negative days  " should "return no customer whom we have sent the campaign" in {
    val mailTypeCustomers = pastCampaignCheck.getCampaignCustomers(pastCampaignData, 47, -20)
    assert(mailTypeCustomers == null)
  }

  "Past campaign Data with 47 mail type" should "return no customer whom we have sent the campaign" in {
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
    val date = format.parse("2015-07-09 00:00:08.0")
    val ndays = TimeUtils.daysFromToday(date).toInt
    val mailTypeCustomers = pastCampaignCheck.getCampaignCustomers(pastCampaignData, 47, ndays)
    assert(mailTypeCustomers.count == 0)
  }

  "Past campaign Data with 46 mail type" should "return one customer whom we have sent the campaign" in {
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
    val date = format.parse("2015-07-09 00:00:08.0")
    val ndays = TimeUtils.daysFromToday(date).toInt
    val mailTypeCustomers = pastCampaignCheck.getCampaignCustomers(pastCampaignData, 46, ndays)
    assert(mailTypeCustomers.count == 1)
  }

  "No past campaign Data to past campaign check" should "return null customer whom we have sent the campaign" in {
    val campaignNotSendCustomers = pastCampaignCheck.campaignCheck(null, customerSelected, 46, 20)
    assert(campaignNotSendCustomers == null)
  }

  "No customer selected Data to past campaign check" should "return null customer whom we have not sent the campaign" in {
    val campaignNotSendCustomers = pastCampaignCheck.campaignCheck(pastCampaignData, null, 46, 20)
    assert(campaignNotSendCustomers == null)
  }

  "No mail Type to past campaign check" should "return null customer whom we have sent not the campaign" in {
    val campaignNotSendCustomers = pastCampaignCheck.campaignCheck(pastCampaignData, customerSelected, 0, 20)
    assert(campaignNotSendCustomers == null)
  }

  "Negative days to past campaign check" should "return null customer whom we have sent not the campaign" in {
    val campaignNotSendCustomers = pastCampaignCheck.campaignCheck(pastCampaignData, customerSelected, 47, -20)
    assert(campaignNotSendCustomers == null)
  }

  "Past campaign Data with 46 mail type to past campaign check" should "return one customer whom we have not sent the campaign" in {
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
    val date = format.parse("2015-07-09 00:00:08.0")
    val ndays = TimeUtils.daysFromToday(date).toInt
    val campaignNotSendCustomers = pastCampaignCheck.campaignCheck(pastCampaignData, customerSelected, 46, ndays)
    assert(campaignNotSendCustomers.count == 1)
  }

  "Past campaign Data with 47 mail type to past campaign check" should "return two customer whom we have  not sent the campaign" in {
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
    val date = format.parse("2015-07-09 00:00:08.0")
    val ndays = TimeUtils.daysFromToday(date).toInt
    val campaignNotSendCustomers = pastCampaignCheck.campaignCheck(pastCampaignData, customerSelected, 47, ndays)
    assert(campaignNotSendCustomers.count == 2)
  }

}
