package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.{ SharedSparkContext, Spark }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FlatSpec

/**
 * Acart test cases
 */
class ACartTest extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var testDataFrame: DataFrame = _
  @transient var salesCartOld: DataFrame = _
  @transient var salesCartData: DataFrame = _
  @transient var orderItemData: DataFrame = _
  @transient var orderData: DataFrame = _
  @transient var orderItemData1: DataFrame = _
  @transient var orderData1: DataFrame = _
  var cartCampaign: ACart = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    cartCampaign = new ACart()
    salesCartOld = sqlContext.read.json("src/test/resources/salescart/CustomerOrderHistory.json")
    salesCartData = sqlContext.read.json("src/test/resources/campaign/acart_campaigns/sales_cart.json")
    orderData = sqlContext.read.json("src/test/resources/campaign/acart_campaigns/sales_order.json")
    orderItemData = sqlContext.read.json("src/test/resources/campaign/acart_campaigns/sales_order_item.json")
    orderData1 = sqlContext.read.json("src/test/resources/campaign/acart_campaigns/sales_order1.json")
    orderItemData1 = sqlContext.read.json("src/test/resources/campaign/acart_campaigns/sales_order_item1.json")
    //testDataFrame = sqlContext.read.json("src/test/resources/SalesCartEmpty.json")
  }



//  "Null DataFrame" should "return null" in {
//    val customerSelected = cartCampaign.customerSelection(null)
//    assert(customerSelected == null)
//  }
//
//  "Customer Data with no Abundant Cart Customers" should "return dataframe with no records" in {
//    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartEmpty.json")
//    val customerSelected = cartCampaign.customerSelection(testDataFrame)
//    assert(customerSelected.count() == 0)
//  }
//
//  "Customer Data with  total 4 Customers" should "return dataframe of 2 customers having abundantCart" in {
//    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartBasic.json")
//    val customerSelected = cartCampaign.customerSelection(testDataFrame)
//    assert(customerSelected.count() == 2)
//  }

  "Null Customer DataFrame" should "return null" in {
    val customerSelected = cartCampaign.customerSkuFilter(null)
    assert(customerSelected == null)
  }

  "Null sku " should "return null filtered sku" in {
    val sku = cartCampaign.skuPriceFilter(null, 123.4, 124.3)
    assert(sku == null)
  }

  "Null Price " should "return null filtered sku" in {
    val sku = cartCampaign.skuPriceFilter("2123w21asc", null, 124.3)
    assert(sku == null)
  }

  "If Today sku Price  is more than order date sku Price" should "return null filtered sku" in {
    val sku = cartCampaign.skuPriceFilter("2123w21asc", 122.4, 124.60)
    assert(sku == null)
  }

  "If Today sku Price  is more than order date sku Price" should "return 2123w21asc filtered sku" in {
    val sku = cartCampaign.skuPriceFilter("2123w21asc", 124.50, 122.70)
    assert(sku == "2123w21asc")
  }

//  "Out of 5 records " should "return 2 records" in {
//    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartBasic.json")
//    val filteredCustomerData = cartCampaign.customerSkuFilter(testDataFrame)
//    assert(filteredCustomerData.count == 3)
//  }

  "null order Data" should "return null DataFrame" in {
    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartFilteredSku.json")
    val filteredOrderData = cartCampaign.customerOrderFilter(testDataFrame, null)
    assert(filteredOrderData == null)
  }

  "null sku filtered Data" should "return null DataFrame" in {
    val filteredOrderData = cartCampaign.customerOrderFilter(null, salesCartOld)
    assert(filteredOrderData == null)
  }

//  "skuData check wth last 30 order data " should "return skus which are not ordered" in {
//    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartFilteredSku.json")
//    val filteredOrderData = cartCampaign.customerOrderFilter(testDataFrame, cartCampaign.groupCustomerData(salesCartOld))
//    filteredOrderData.collect().foreach(println)
//    assert(filteredOrderData != null)
//  }

//  "customer Grouped data " should "return sku grouped data" in {
//    val groupedData = cartCampaign.groupCustomerData(salesCartOld)
//    groupedData.collect().foreach(println)
//    assert(groupedData.count() == 4)
//  }


  "No sales cart Input Data" should "return no selected customers" in {
    val customerSelected = cartCampaign.customerSelection(null,orderData,orderItemData)
    assert(customerSelected == null)
  }

  "No order Data " should "return no selected customers" in {
    val customerSelected = cartCampaign.customerSelection(salesCartData,null,orderItemData)
    assert(customerSelected == null)
  }

  "No order Item Data " should "return no selected customers" in {
    val customerSelected = cartCampaign.customerSelection(salesCartData,orderData,null)
    assert(customerSelected == null)
  }

  "sales cart with order Item Data  Data" should "return one selected customers because sku was bought after adding in the cart" in {
    val customerSelected = cartCampaign.customerSelection(salesCartData,orderData,orderItemData)
    assert(customerSelected.count == 1)
  }

  "sales cart with order Item Data  Data" should "return two selected customers because sku was bought before adding in the cart" in {
    val customerSelected = cartCampaign.customerSelection(salesCartData,orderData1,orderItemData1)
    assert(customerSelected.count == 2)
  }

}
