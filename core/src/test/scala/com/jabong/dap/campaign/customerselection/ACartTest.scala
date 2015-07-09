package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.{SharedSparkContext, Spark}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FlatSpec


/**
 * Created by jabong1145 on 17/6/15.
 */
class ACartTest extends FlatSpec with SharedSparkContext{

  @transient var sqlContext: SQLContext = _
  @transient var testDataFrame : DataFrame = _
  @transient var orderDataFrame : DataFrame = _
  var cartCampaign :ACart = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    cartCampaign=new ACart(sqlContext)
    orderDataFrame = sqlContext.read.json("src/test/resources/salescart/CustomerOrderHistory.json")
    //testDataFrame = sqlContext.read.json("src/test/resources/SalesCartEmpty.json")
  }

  override def afterAll(){
    super.afterAll()
  }

  "Null DataFrame" should "return null" in {
    val customerSelected = cartCampaign.customerSelection(null)
    assert(customerSelected==null)
  }

  "Customer Data with no Abundant Cart Customers" should "return dataframe with no records" in {
    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartEmpty.json")
    val customerSelected = cartCampaign.customerSelection(testDataFrame)
    assert(customerSelected.count()==0)
  }

  "Customer Data with  total 4 Customers" should "return dataframe of 2 customers having abundantCart" in {
    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartBasic.json")
    val customerSelected = cartCampaign.customerSelection(testDataFrame)
    assert(customerSelected.count()==2)
  }

  "Null Customer DataFrame" should "return null" in {
    val customerSelected = cartCampaign.customerSkuFilter(null)
    assert(customerSelected==null)
  }

  "Null sku " should "return null filtered sku" in {
    val sku = cartCampaign.skuPriceFilter(null,123.4,124.3)
    assert(sku==null)
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


  "Out of 5 records " should "return 2 records" in {
    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartBasic.json")
    val filteredCustomerData = cartCampaign.customerSkuFilter(testDataFrame)
    assert(filteredCustomerData.count==3)
  }


  "null order Data" should "return null DataFrame" in {
    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartFilteredSku.json")
    val filteredOrderData = cartCampaign.customerOrderFilter(testDataFrame,null)
    assert(filteredOrderData==null)
  }


  "null sku filtered Data" should "return null DataFrame" in {
    val filteredOrderData = cartCampaign.customerOrderFilter(null,orderDataFrame)
    assert(filteredOrderData==null)
  }

  "skuData check wth last 30 order data " should "return skus which are not ordered" in {
    testDataFrame = sqlContext.read.json("src/test/resources/salescart/SalesCartFilteredSku.json")
    val filteredOrderData = cartCampaign.customerOrderFilter(testDataFrame, cartCampaign.groupCustomerData(orderDataFrame))
    filteredOrderData.collect().foreach(println)
    assert(filteredOrderData!=null)
  }

  "customer Grouped data " should "return sku grouped data" in {
    val groupedData = cartCampaign.groupCustomerData(orderDataFrame)
    groupedData.collect().foreach(println)
    assert(groupedData.count()==4)
  }



}
