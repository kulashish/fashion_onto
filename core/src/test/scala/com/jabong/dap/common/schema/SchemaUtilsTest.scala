package com.jabong.dap.common.schema

import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, TestSchema }
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.scalatest.{ FlatSpec, Matchers }

/**
 * Created by pooja on 28/7/15.
 */
class SchemaUtilsTest extends FlatSpec with Matchers with Serializable with SharedSparkContext {

  @transient var campaignsOutDataFull: DataFrame = _
  @transient var campaignsOutData: DataFrame = _
  @transient var campaignsData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    campaignsOutDataFull = JsonUtils.readFromJson("campaigns/manager", "campaign_output", TestSchema.campaignOutput)

    campaignsOutData = campaignsOutDataFull.select(
      CampaignMergedFields.CUSTOMER_ID,
      CampaignMergedFields.CAMPAIGN_MAIL_TYPE,
      CampaignMergedFields.REF_SKU1
    )

    campaignsData = JsonUtils.readFromJson("campaigns/processor", "campaignInput", TestSchema.campaignPriorityOutput)
  }

  "isSchemaEqual" should "return true" in {
    SchemaUtils.isSchemaEqual(Schema.customer, Schema.customer) should be (true)
  }

  "isSchemaEqual" should "return false" in {
    SchemaUtils.isSchemaEqual(Schema.customer, Schema.nls) should be (false)
  }

  "addColumns" should "add columns" in {
    val res = SchemaUtils.addColumns(campaignsOutData, TestSchema.campaignPriorityOutput)
    res.printSchema()
    println(TestSchema.campaignPriorityOutput.treeString)
    assert(res.columns.length == 7)
    assert(SchemaUtils.isSchemaEqual(res.schema, TestSchema.campaignPriorityOutput))
  }

  "dropColumns" should "drop columns" in {
    campaignsOutDataFull.printSchema()
    val res = SchemaUtils.dropColumns(campaignsOutDataFull, campaignsOutData.schema)
    println(campaignsOutData.printSchema())
    res.printSchema()
    assert(res.columns.length == 3)
    assert(SchemaUtils.isSchemaEqual(res.schema, campaignsOutData.schema))
  }

  "changeSchema" should "add columns" in {
    val res = SchemaUtils.changeSchema(campaignsOutData, TestSchema.campaignPriorityOutput)
    res.printSchema()
    println(TestSchema.campaignPriorityOutput.treeString)
    assert(res.columns.length == 7)
    assert(SchemaUtils.isSchemaEqual(res.schema, TestSchema.campaignPriorityOutput))
  }

  "changeSchema" should "drop columns" in {
    campaignsOutDataFull.printSchema()
    val res = SchemaUtils.changeSchema(campaignsOutDataFull, campaignsOutData.schema)
    println(campaignsOutData.printSchema())
    res.printSchema()
    assert(res.columns.length == 3)
    assert(SchemaUtils.isSchemaEqual(res.schema, campaignsOutData.schema))
  }

  "changeSchema" should "add and drop columns" in {
    campaignsOutDataFull.printSchema()
    println(TestSchema.campaignPriorityOutput.treeString)
    val res = SchemaUtils.changeSchema(campaignsOutDataFull, TestSchema.campaignPriorityOutput)
    res.printSchema()
    assert(res.columns.length == 7)
    assert(SchemaUtils.isSchemaEqual(res.schema, TestSchema.campaignPriorityOutput))
  }

}
