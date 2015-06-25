package com.jabong.dap.campaign.recommendation

import com.jabong.dap.campaign.common.ACartCampaign
import com.jabong.dap.common.Constants.Variables.ProductVariables
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
    assert(topSkus.count()==4)
  }


  "Few days old order Items data input  " should "return no top skus sold for today  " in {
    val topSkus =  basicRecommender.topProductsSold(orderItemDataFrame,0)
    assert(topSkus==null)
  }



  "top skus  input and itr" should "return sku complete data" in {
    val recInput =  basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame,10),itrDataFrame)
    val recInputbrand = recInput.filter(ProductVariables.Sku+"='XW574WA35AZGINDFAS'").select(ProductVariables.Brand).collect()(0)(0).toString()
    assert(recInputbrand=="adidas")
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



}
