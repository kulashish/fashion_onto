package com.jabong.dap.campaign.utils

import java.sql.Timestamp
import java.text.{ DateFormat, SimpleDateFormat }
import java.util.Calendar

import com.jabong.dap.campaign.skuselection.CancelReTarget
import com.jabong.dap.common.constants.variables.{ SalesOrderVariables, ProductVariables }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.order.variables.SalesOrder
import org.apache.spark.sql.{ Row, DataFrame, SQLContext }
import org.scalatest.FlatSpec

/**
 * Utilities test class
 */
class CampaignUtilsTest extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var refSkuInput: DataFrame = _
  @transient var customerSelected: DataFrame = _
  @transient var salesOrder: DataFrame = _
  @transient var salesOrderItem: DataFrame = _
  @transient var customerSelectedTime: DataFrame = _
  @transient var customerSelectedShortlist: DataFrame = _

  val calendar = Calendar.getInstance()
  calendar.add(Calendar.DATE, -1)
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
  val testDate = Timestamp.valueOf(dateFormat.format(calendar.getTime))

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    refSkuInput = JsonUtils.readFromJson("campaign", "ref_sku_input", Schema.refSkuInput)
    customerSelected = sqlContext.read.json("src/test/resources/campaign/campaign_utils/customer_selected.json")
    customerSelectedShortlist = sqlContext.read.json("src/test/resources/campaign/campaign_utils/customer_selected_shortlist.json")
    salesOrder = sqlContext.read.json("src/test/resources/campaign/campaign_utils/sales_order_placed.json")
    salesOrderItem = sqlContext.read.json("src/test/resources/campaign/campaign_utils/sales_item_bought.json")
    customerSelectedTime = sqlContext.read.json("src/test/resources/campaign/campaign_utils/customer_filtered_time.json")

  }

  "Yesterdays date " should "return 1 in day diff" in {
    val diff = CampaignUtils.currentTimeDiff(testDate, "days")
    assert(diff == 1)
  }

  "Yesterdays date " should "return number of hours in time diff" in {
    val diff = CampaignUtils.currentTimeDiff(testDate, "hours")
    assert(diff >= 23 && diff <= 24)
  }

  "Yesterdays date " should "return number of minutes in time diff" in {
    val diff = CampaignUtils.currentTimeDiff(testDate, "minutes")
    assert(diff <= 1441)
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
    val refSkuValues = refSkus.filter(SalesOrderVariables.FK_CUSTOMER + "=16509341").select(ProductVariables.SKU_LIST).collect()(0)(0).asInstanceOf[List[(Double, String)]]
    val expectedData = Row(500.0, "IM794WA05ZGKINDFAS-4434414")
    assert(refSkuValues.head === expectedData)
    assert(refSkuValues.size == 2)
  }

  "Generate reference skus with refernce sku input " should "return max 2 reference skus per customer sorted with price and take care of duplicate skus" in {
    val refSkus = CampaignUtils.generateReferenceSkus(refSkuInput, 2)
    val refSkuFirst = refSkus.filter(SalesOrderVariables.FK_CUSTOMER + "=5242607").select(ProductVariables.SKU_LIST).collect()(0)(0).asInstanceOf[List[(Double, String)]]
    val expectedData = Row(200.0, "VA613SH24VHFINDFAS-3716539")
    assert(refSkuFirst.head === (expectedData))
    //  assert(refSkuFirst.head._2 == "VA613SH24VHFINDFAS-3716539")
  }

  "Generate reference skus with refernce sku input " should "return max 1 reference skus per customer sorted with price" in {
    val refSkus = CampaignUtils.generateReferenceSkus(refSkuInput, 1)
    val refSkuFirst = refSkus.filter(SalesOrderVariables.FK_CUSTOMER + "=8552648").select(ProductVariables.SKU_LIST).collect()(0)(0).asInstanceOf[List[(Double, String)]]
    val expectedData = Row(2095.0, "GE160BG56HMHINDFAS-2211538")
    assert(refSkuFirst.head === expectedData)
    assert(refSkuFirst.size == 1)
  }

  "No input Data for sku simple Not Bought" should "return null" in {
    val skuNotBought = CampaignUtils.skuSimpleNOTBought(null, salesOrder, salesOrderItem)
    assert(skuNotBought == null)
  }

  "input Data  with order data " should "return sku simple not bought till now" in {
    val skuNotBought = CampaignUtils.skuSimpleNOTBought(customerSelected, salesOrder, salesOrderItem)
    assert(skuNotBought.count() == 1)
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
    val filteredData = CampaignUtils.getTimeBasedDataFrame(null, field, after, before)
    assert(filteredData == null)
  }

  "Different before and after format" should "return no filtered frame" in {
    val after = "2015-07-02 22:36:58"
    val before = "2015-07-12 22:36:58.0"
    val field = "updated_at"
    val filteredData = CampaignUtils.getTimeBasedDataFrame(customerSelectedTime, field, after, before)
    assert(filteredData == null)
  }

  "null Field " should "return no filtered frame" in {
    val after = "2015-07-02 22:36:58"
    val before = "2015-07-12 22:36:58.0"
    val filteredData = CampaignUtils.getTimeBasedDataFrame(customerSelectedTime, null, after, before)
    assert(filteredData == null)
  }

  "Field doesn't exist in data frame schema" should "return no filtered frame" in {
    val after = "2015-07-02 22:36:58"
    val before = "2015-07-12 22:36:58.0"
    val filteredData = CampaignUtils.getTimeBasedDataFrame(customerSelectedTime, null, after, before)
    assert(filteredData == null)
  }

  "order data from 2015-07-02 22:36:58.0 to 2015-07-12 22:36:58.0 " should "return two records based on before and after" in {
    val after = "2015-07-02 22:36:58.0"
    val before = "2015-07-12 22:36:58.0"
    val field = "updated_at"
    val filteredData = CampaignUtils.getTimeBasedDataFrame(customerSelectedTime, field, after, before)
    assert(filteredData.count == 2)
  }

  "order data from 2015-07-02 22:36:59.0 to 2015-07-12 22:36:58.0 " should "return one record based on before and after" in {
    val after = "2015-07-02 22:36:59.0"
    val before = "2015-07-12 22:36:58.0"
    val field = "updated_at"
    val filteredData = CampaignUtils.getTimeBasedDataFrame(customerSelectedTime, field, after, before)
    assert(filteredData.count == 1)
  }
  //
  //  "Given data format " should "return current time in that format" in {
  //    val currentTime = CampaignUtils.now("yyyy/mm/dd")
  //    assert(currentTime=="2015/07/13")
  //  }
}
