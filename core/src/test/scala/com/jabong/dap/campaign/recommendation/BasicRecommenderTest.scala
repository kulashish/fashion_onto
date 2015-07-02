package com.jabong.dap.campaign.recommendation

import com.jabong.dap.campaign.common.ACartCampaign
import com.jabong.dap.common.constants.variables.ProductVariables
import com.jabong.dap.common.{SharedSparkContext, Spark}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FlatSpec

/**
 * Created by jabong1145 on 23/6/15.
 */
class BasicRecommenderTest extends FlatSpec with SharedSparkContext{

  @transient var sqlContext: SQLContext = _
  @transient var hiveContext: SQLContext = _

  @transient var itrDataFrame : DataFrame = _
  @transient var orderItemDataFrame : DataFrame = _
  var basicRecommender :BasicRecommender = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    hiveContext = Spark.getHiveContext()

    basicRecommender=new BasicRecommender()
    orderItemDataFrame = hiveContext.read.json("src/test/resources/SalesCart/OrderItemHistory.json")
    itrDataFrame = hiveContext.read.json("src/test/resources/SalesCart/itrData.json")

    //testDataFrame = sqlContext.read.json("src/test/resources/SalesCartEmpty.json")
  }


  "No gender value" should "return NO recommended gender" in {
    val recommendedGender =  basicRecommender.getRecommendationGender(null)
    assert(recommendedGender==null)
  }

  "MEN gender value" should "return MEN,UNISEX recommended gender" in {
    val recommendedGender =  basicRecommender.getRecommendationGender("MEN")
    assert(recommendedGender=="MEN,UNISEX")
  }



  "Null order data" should "return null dataframe" in {
    assert(basicRecommender.topProductsSold(null,10)==null)
  }

  "Negative days " should "return null dataframe" in {
    assert(basicRecommender.topProductsSold(orderItemDataFrame,-10)==null)
  }


  "sku simple GE160BG56HMHINDFAS-2211538" should "return only sku GE160BG56HMHINDFAS" in {
    assert(basicRecommender.simpleToSku("GE160BG56HMHINDFAS-2211538")=="GE160BG56HMHINDFAS")
  }


  "sku simple GE160BG56HMHINDFAS" should "return only sku GE160BG56HMHINDFAS" in {
    assert(basicRecommender.simpleToSku("GE160BG56HMHINDFAS")=="GE160BG56HMHINDFAS")
  }


  "sku simple null" should "return null" in {
    assert(basicRecommender.simpleToSku(null)==null)
  }

  "Order Items data input  " should "return sku and sorted by quantity dataframe" in {
    val topSkus =  basicRecommender.topProductsSold(orderItemDataFrame,10)
    assert(topSkus.count()==5)
  }


  "Few days old order Items data input  " should "return no top skus sold for today  " in {
    val topSkus =  basicRecommender.topProductsSold(orderItemDataFrame,0)
    assert(topSkus==null)
  }



  "top skus  input and itr" should "return sku complete data" in {
    val recInput =  basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame,10),itrDataFrame)
    val recInputbrand = recInput.filter(ProductVariables.SKU +"='XW574WA35AZGINDFAS'").select(ProductVariables.BRAND).collect()(0)(0).toString()
    assert(recInputbrand=="adidas")
  }

  " skus BR828MA28TMPINDFAS input and itr" should "return WOMEN GENDER" in {
    val recInput =  basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame,10),itrDataFrame)
    val recInputbrand = recInput.filter(ProductVariables.SKU+"='BR828MA28TMPINDFAS'").select(ProductVariables.GENDER).collect()(0)(0).toString()
    assert(recInputbrand=="WOMEN")
  }



  "No top skus input and itr" should "return null data" in {
    val recInput =  basicRecommender.skuCompleteData(null,itrDataFrame)
    assert(recInput==null)
  }


  "No Recommendation input skus" should "return No recommendation output" in {
    val recOut = basicRecommender.genRecommend(null)
    assert(recOut==null)
  }


  "5 recommendation input skus" should "create recommendation based on brand mvp and gender" in {
    val recOut = basicRecommender.genRecommend(basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame,10),itrDataFrame))
    assert(recOut!=null)
  }


  "No category for inventory last week not sold " should "return false means should get filtered" in {
    val inventorySoldStatus = basicRecommender.inventoryWeekNotSold(null,40,20)
    assert(inventorySoldStatus==false)
  }

  "Stock less than weekly average " should "return false means should get filtered" in {
    val inventorySoldStatus = basicRecommender.inventoryWeekNotSold("SUNGLASSES",20,30)
    assert(inventorySoldStatus==false)
  }

  "Sunglasses category has stock more than double the weekly average " should "return true" in {
    val inventorySoldStatus = basicRecommender.inventoryWeekNotSold("SUNGLASSES",50,20)
    assert(inventorySoldStatus==true)
  }

  "Sunglasses category has stock less than double the weekly average " should "return false" in {
    val inventorySoldStatus = basicRecommender.inventoryWeekNotSold("SUNGLASSES",35,20)
    assert(inventorySoldStatus==false)
  }

}
