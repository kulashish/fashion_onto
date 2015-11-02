package com.jabong.dap.campaign.utils

import java.io.File

import com.jabong.dap.common.constants.campaign.{ CampaignMergedFields, SkuSelection }
import com.jabong.dap.common.constants.variables.{ ItrVariables, ProductVariables, SalesOrderVariables }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ TestSchema, SharedSparkContext, Spark, TestConstants }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.scalatest.FlatSpec
import com.jabong.dap.common.Utils

/**
 * Utilities test class
 */
class CampaignUtilsTest extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var refSkuInput: DataFrame = _
  @transient var refSkuInputPush: DataFrame = _
  @transient var customerSelected: DataFrame = _
  @transient var salesOrder: DataFrame = _
  @transient var salesOrderItem: DataFrame = _
  @transient var customerSelectedTime: DataFrame = _
  @transient var customerSelectedShortlist: DataFrame = _
  @transient var dfCustomerPageVisit: DataFrame = _
  @transient var dfCustomer: DataFrame = _

  @transient var dfCustomerProductShortlist: DataFrame = _
  @transient var dfItr30DayData: DataFrame = _
  @transient var dfYesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    refSkuInput = JsonUtils.readFromJson(DataSets.CAMPAIGNS, "ref_sku_input", TestSchema.refSkuInput)
    refSkuInputPush = JsonUtils.readFromJson(DataSets.CAMPAIGNS, "ref_sku_input_push", TestSchema.refSkuInput)
    customerSelected = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/campaign_utils", "customer_selected")
    customerSelectedShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/campaign_utils", "customer_selected_shortlist")
    salesOrder = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/campaign_utils", "sales_order_placed")
    salesOrderItem = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/campaign_utils", "sales_item_bought")
    customerSelectedTime = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/campaign_utils", "customer_filtered_time")

    dfCustomerPageVisit = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.SURF, TestConstants.CUSTOMER_PAGE_VISIT, TestSchema.customerPageVisitSkuLevel)
    dfCustomer = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.SURF, DataSets.CUSTOMER, Schema.customer)

    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.RESULT_CUSTOMER_PRODUCT_SHORTLIST, TestSchema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.YESTERDAY_ITR_DATA, Schema.itr)

  }

  "Generate reference skus with input null data " should "no reference skus" in {
    val refSkus = CampaignUtils.generateReferenceSkus(null, 2)
    assert(refSkus == null)
  }

  "Generate reference skus with input 0 number of ref skus to generate  " should "no reference skus" in {
    val refSkus = CampaignUtils.generateReferenceSkus(refSkuInput, 0)
    assert(refSkus == null)
  }

  "Generate reference skus with refernce sku input " should "return max 2 reference skus per customer sorted with price" in {
    val refSkus = CampaignUtils.generateReferenceSkus(refSkuInput, 2)
    val refSkuValues = refSkus.filter(SalesOrderVariables.FK_CUSTOMER + "=16509341").select(CampaignMergedFields.REF_SKUS)
      .collect()(0)(0).asInstanceOf[List[(Double, String, String, String, String, String)]]
    assert(refSkuValues.size == 2)
  }

  "Generate reference skus with refernce sku input " should "return max 2 reference skus per customer sorted with price and take care of duplicate skus" in {
    val refSkus = CampaignUtils.generateReferenceSkus(refSkuInput, 2)
    val refSkuFirst = refSkus.filter(SalesOrderVariables.FK_CUSTOMER + "=5242607").select(CampaignMergedFields.REF_SKUS).collect()(0)(0).asInstanceOf[List[(Double, String, String, String, String, String)]]
    //val expectedData = Row(200.0, "VA613SH24VHFINDFAS-3716539")
    assert(refSkuFirst.size === 2)
    //  assert(refSkuFirst.head._2 == "VA613SH24VHFINDFAS-3716539")
  }

  "Generate reference skus with refernce sku input " should "return max 1 reference skus per customer sorted with price" in {
    val refSkus = CampaignUtils.generateReferenceSkus(refSkuInput, 1)
    val refSkuFirst = refSkus.filter(SalesOrderVariables.FK_CUSTOMER + "=8552648").select(CampaignMergedFields.REF_SKUS).collect()(0)(0).asInstanceOf[List[(Double, String, String, String, String, String)]]
    val expectedData = Row(2095.0, "GE160BG56HMHINDFAS-2211538")
    // assert(refSkuFirst.head === expectedData)
    assert(refSkuFirst.size == 1)
  }

  "Generate reference sku with refernce sku input " should "return only one reference sku per customer sorted with price" in {
    val refSkus = CampaignUtils.generateReferenceSku(refSkuInputPush, 1)
    val refSkuFirst = refSkus.filter(SalesOrderVariables.FK_CUSTOMER + "=8552648")
    // val expectedData = Row(2095.0, "GE160BG56HMHINDFAS-2211538")
    // assert(refSkuFirst.head === expectedData)
    assert(refSkuFirst.count == 1)
  }

  "No input Data for sku simple Not Bought" should "return null" in {
    val skuNotBought = CampaignUtils.skuSimpleNOTBoughtWithoutPrice(null, salesOrder, salesOrderItem)
    assert(skuNotBought == null)
  }

  "input Data  with order data " should "return sku simple not bought till now" in {
    val skuNotBought = CampaignUtils.skuSimpleNOTBoughtWithoutPrice(customerSelected, salesOrder, salesOrderItem)
    assert(skuNotBought.count() == 2)
  }

  "No input Data for sku Not Bought" should "return null" in {
    val skuNotBought = CampaignUtils.skuNotBought(null, salesOrder, salesOrderItem)
    assert(skuNotBought == null)
  }

  "input Data  with order data " should "return sku not bought till now" in {
    val skuNotBought = CampaignUtils.skuNotBought(customerSelectedShortlist, salesOrder, salesOrderItem)
    assert(skuNotBought.count() == 2)
  }

  "No order data from 2015-07-02 22:36:58.0 to 2015-07-12 22:36:58.0 " should "return no filtered frame" in {
    val after = "2015-07-02 22:36:58.0"
    val before = "2015-07-12 22:36:58.0"
    val field = "updated_at"
    val filteredData = Utils.getTimeBasedDataFrame(null, field, after, before)
    assert(filteredData == null)
  }

  "Different before and after format" should "return no filtered frame" in {
    val after = "2015-07-02 22:36:58"
    val before = "2015-07-12 22:36:58.0"
    val field = "updated_at"
    val filteredData = Utils.getTimeBasedDataFrame(customerSelectedTime, field, after, before)
    assert(filteredData == null)
  }

  "null Field " should "return no filtered frame" in {
    val after = "2015-07-02 22:36:58"
    val before = "2015-07-12 22:36:58.0"
    val filteredData = Utils.getTimeBasedDataFrame(customerSelectedTime, null, after, before)
    assert(filteredData == null)
  }

  "Field doesn't exist in data frame schema" should "return no filtered frame" in {
    val after = "2015-07-02 22:36:58"
    val before = "2015-07-12 22:36:58.0"
    val filteredData = Utils.getTimeBasedDataFrame(customerSelectedTime, null, after, before)
    assert(filteredData == null)
  }

  "order data from 2015-07-02 22:36:58.0 to 2015-07-12 22:36:58.0 " should "return two records based on before and after" in {
    val after = "2015-07-02 22:36:58.0"
    val before = "2015-07-12 22:36:58.0"
    val field = "updated_at"
    val filteredData = Utils.getTimeBasedDataFrame(customerSelectedTime, field, after, before)
    assert(filteredData.count == 2)
  }

  "order data from 2015-07-02 22:36:59.0 to 2015-07-12 22:36:58.0 " should "return one record based on before and after" in {
    val after = "2015-07-02 22:36:59.0"
    val before = "2015-07-12 22:36:58.0"
    val field = "updated_at"
    val filteredData = Utils.getTimeBasedDataFrame(customerSelectedTime, field, after, before)
    assert(filteredData.count == 1)
  }
  //
  //  "Given data format " should "return current time in that format" in {
  //    val currentTime = CampaignUtils.now("yyyy/mm/dd")
  //    assert(currentTime=="2015/07/13")
  //  }

  "getMappingCustomerEmailToCustomerId(a,b): All Data Frame " should "null" in {

    val result = CampaignUtils.getMappingCustomerEmailToCustomerId(null, null)

    assert(result == null)

  }

  "getMappingCustomerEmailToCustomerId(a,b): count " should "3" in {

    val result = CampaignUtils.getMappingCustomerEmailToCustomerId(dfCustomerPageVisit, dfCustomer)

    assert(result.count() == 10)
  }

  //==========================================shortListSkuFilter()======================================================

  "shortListSkuFilter: Data Frame" should "match to resultant Data Frame" in {

    var itr30Day = dfItr30DayData

    itr30Day = dfItr30DayData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE,
      col(ItrVariables.CREATED_AT) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    var yesterdayItrData = dfYesterdayItrData

    yesterdayItrData = yesterdayItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE,
      col(ItrVariables.CREATED_AT) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    val result = CampaignUtils.shortListSkuItrJoin(dfCustomerProductShortlist, yesterdayItrData, itr30Day)
    //      .limit(30).collect().toSet
    //
    //    //                           result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_sku_filter" + ".json")
    //
    //    val dfShortListSkuFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + File.separator + DataSets.SKU_SELECTION + File.separator + DataSets.ITEM_ON_DISCOUNT, "result_shortlist_sku_filter", Schema.resultSkuFilter)
    //      .collect().toSet

    assert(result.count() == 4)

  }

  //=====================================shortListSkuSimpleFilter()=====================================================

  "shortListSkuSimpleFilter: Data Frame" should "match to resultant Data Frame" in {

    var yesterdayItrData = dfYesterdayItrData

    yesterdayItrData = yesterdayItrData.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )

    val result = CampaignUtils.shortListSkuSimpleItrJoin(dfCustomerProductShortlist, yesterdayItrData)
    //      .limit(30).collect().toSet

    //                   result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_sku_simple_filter" + ".json")

    //    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + File.separator + DataSets.SKU_SELECTION + File.separator + DataSets.ITEM_ON_DISCOUNT, "result_shortlist_sku_simple_filter", Schema.resultSkuSimpleFilter)
    //      .collect().toSet

    assert(result.count() == 0)

  }

  "Generate reference skus with refernce sku input " should "return max 2 reference skus with acart utl per customer sorted with price and take care of duplicate skus" in {
    val refSkus = CampaignUtils.generateReferenceSkusForAcart(refSkuInput, 2)
    val refSkuFirst = refSkus.filter(SalesOrderVariables.FK_CUSTOMER + "=5242607").select(CampaignMergedFields.REF_SKUS).collect()(0)(0).asInstanceOf[List[(Double, String, String, String, String, String)]]
    //val expectedData = Row(200.0, "VA613SH24VHFINDFAS-3716539")
    assert(refSkuFirst.size === 2)
    //  assert(refSkuFirst.head._2 == "VA613SH24VHFINDFAS-3716539")
  }
}
