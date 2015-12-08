package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.{ Spark, SharedSparkContext }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.scalatest.{ FeatureSpec, GivenWhenThen, FlatSpec }

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
 * Created by samathashetty on 27/11/15.
 */
class WinbackDataTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {
  @transient var sqlContext: SQLContext = _

  @transient var salesOrder: DataFrame = _
  @transient var crmTicketDetails: DataFrame = _
  @transient var crmTicketMaster: DataFrame = _
  @transient var crmTicketStatLog: DataFrame = _
  @transient var cmr: DataFrame = _

  override def beforeAll() = {
    super.beforeAll()

    CampaignOutput.setTestMode(true)
    sqlContext = Spark.getSqlContext()
    salesOrder = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/geo_campaign", "sales_order", Schema.salesOrder)
    crmTicketDetails = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/winback", "crm_ticket_details", Schema.crmTicketDetails)
    crmTicketMaster = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/winback", "crm_ticket_master", Schema.crmTicketMaster)
    crmTicketStatLog = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/winback", "crm_ticket_statuslog", Schema.crmTktStatusLog)
    cmr = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "cmr", Schema.cmr)

  }

  feature("Run winback campaign") {
    scenario("R=un win back campaign") {
      Given("salesOrder, crm tables")

      WinbackData.dateStr = "2015/11/24"
      val dfMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]
      dfMap.put("crmTicketMasterIncr", crmTicketMaster)
      dfMap.put("crmTicketDetailsIncr", crmTicketDetails)

      dfMap.put("crmTicketStatLogIncr", crmTicketStatLog)
      dfMap.put("cmrFull", cmr)
      dfMap.put("salesOrder", salesOrder)
      WinbackData.process(dfMap)
    }

  }

}
