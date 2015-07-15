package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.constants.variables.ProductVariables
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FlatSpec

import scala.collection.mutable

/**
 *  basic recommender test cases
 */
class BasicRecommenderTest extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var itrDataFrame: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _
  var basicRecommender: BasicRecommender = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()

    basicRecommender = new BasicRecommender()
    orderItemDataFrame = sqlContext.read.json("src/test/resources/salescart/OrderItemHistory.json")
    itrDataFrame = sqlContext.read.json("src/test/resources/salescart/itrData.json")

    //testDataFrame = sqlContext.read.json("src/test/resources/SalesCartEmpty.json")
  }

  "No gender value" should "return NO recommended gender" in {
    val recommendedGender = basicRecommender.getRecommendationGender(null)
    assert(recommendedGender == null)
  }

  "MEN gender value" should "return MEN,UNISEX recommended gender" in {
    val recommendedGender = basicRecommender.getRecommendationGender("MEN")
    assert(recommendedGender == "MEN,UNISEX")
  }

  "Null order data" should "return null dataframe" in {
    assert(basicRecommender.topProductsSold(null, 10) == null)
  }

  "Negative days " should "return null dataframe" in {
    assert(basicRecommender.topProductsSold(orderItemDataFrame, -10) == null)
  }

  "sku simple GE160BG56HMHINDFAS-2211538" should "return only sku GE160BG56HMHINDFAS" in {
    assert(basicRecommender.simpleToSku("GE160BG56HMHINDFAS-2211538") == "GE160BG56HMHINDFAS")
  }

  "sku simple GE160BG56HMHINDFAS" should "return only sku GE160BG56HMHINDFAS" in {
    assert(basicRecommender.simpleToSku("GE160BG56HMHINDFAS") == "GE160BG56HMHINDFAS")
  }

  "sku simple null" should "return null" in {
    assert(basicRecommender.simpleToSku(null) == null)
  }

  "Order Items data input  " should "return sku and sorted by quantity dataframe" in {
    val topSkus = basicRecommender.topProductsSold(orderItemDataFrame, 10)
    assert(topSkus.count() == 5)
  }

  "Few days old order Items data input  " should "return no top skus sold for today  " in {
    val topSkus = basicRecommender.topProductsSold(orderItemDataFrame, 0)
    assert(topSkus == null)
  }

  "top skus  input and itr" should "return sku complete data" in {
    val recInput = basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame, 10), itrDataFrame)
    val recInputbrand = recInput.filter(ProductVariables.SKU + "='XW574WA35AZGINDFAS'").select(ProductVariables.BRAND).collect()(0)(0).toString()
    assert(recInputbrand == "adidas")
  }

  " skus BR828MA28TMPINDFAS input and itr" should "return WOMEN GENDER" in {
    val recInput = basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame, 10), itrDataFrame)
    val recInputbrand = recInput.filter(ProductVariables.SKU + "='BR828MA28TMPINDFAS'").select(ProductVariables.GENDER).collect()(0)(0).toString()
    assert(recInputbrand == "WOMEN")
  }

  "No top skus input and itr" should "return null data" in {
    val recInput = basicRecommender.skuCompleteData(null, itrDataFrame)
    assert(recInput == null)
  }

  "No Recommendation input skus" should "return No recommendation output" in {
    val recOut = basicRecommender.genRecommend(null, null, null)
    assert(recOut == null)
  }

  "5 recommendation input skus and pivot keys is null" should " return null" in {
    val dataFrameSchema = StructType(Array(
      StructField(ProductVariables.BRICK, StringType, false),
      StructField(ProductVariables.MVP, LongType, false),
      StructField(ProductVariables.GENDER, StringType, false),
      StructField(ProductVariables.RECOMMENDATIONS, ArrayType(StructType(Array(StructField(ProductVariables.QUANTITY, LongType), StructField(ProductVariables.SKU_LIST, StringType))), true))
    ))

    val recOut = basicRecommender.genRecommend(basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame, 10), itrDataFrame), null, dataFrameSchema)
    assert(recOut == null)
  }

  "5 recommendation input skus" should "create recommendation based on brick mvp and gender" in {
    val dataFrameSchema = StructType(Array(
      StructField(ProductVariables.BRICK, StringType, false),
      StructField(ProductVariables.MVP, LongType, false),
      StructField(ProductVariables.GENDER, StringType, false),
      StructField(ProductVariables.RECOMMENDATIONS, ArrayType(StructType(Array(StructField(ProductVariables.QUANTITY, LongType), StructField(ProductVariables.SKU_LIST, StringType))), true))
    ))
    val pivotKeys = Array(ProductVariables.BRICK, ProductVariables.MVP)

    val recOut = basicRecommender.genRecommend(basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame, 10), itrDataFrame), pivotKeys, dataFrameSchema)

    val recommendations = recOut.filter(ProductVariables.BRICK + "= 'mens shoes' and " + ProductVariables.MVP + "='1' and " + ProductVariables.GENDER + "='MEN'")
      .select(ProductVariables.RECOMMENDATIONS).collect()(0)(0).asInstanceOf[mutable.MutableList[(Long, String)]]
    assert(recommendations.length == 1)
  }

  "5 recommendation input skus" should "create recommendation based on brick,brand mvp and gender" in {
    val dataFrameSchema = StructType(Array(
      StructField(ProductVariables.BRICK, StringType, false),
      StructField(ProductVariables.MVP, LongType, false),
      StructField(ProductVariables.BRAND, StringType, false),
      StructField(ProductVariables.GENDER, StringType, false),
      StructField(ProductVariables.RECOMMENDATIONS, ArrayType(StructType(Array(StructField(ProductVariables.QUANTITY, LongType), StructField(ProductVariables.SKU_LIST, StringType))), true))
    ))
    val pivotKeys = Array(ProductVariables.BRICK, ProductVariables.MVP, ProductVariables.BRAND)

    val recOut = basicRecommender.genRecommend(basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame, 10), itrDataFrame), pivotKeys, dataFrameSchema)
    //
    //     val recommendations = recOut.filter(ProductVariables.BRICK+"= 'test' and "+ProductVariables.MVP+"=1 and "+ProductVariables.GENDER+"='UNISEX'")
    //      .select(ProductVariables.RECOMMENDATIONS)
    recOut.printSchema()
    val recommendations = recOut.filter(ProductVariables.BRICK + "= 'test' and " + ProductVariables.MVP + "='1' and " + ProductVariables.GENDER + "='UNISEX'")
      .select(ProductVariables.RECOMMENDATIONS).collect()(0)(0).asInstanceOf[mutable.MutableList[(Long, String)]]
    assert(recommendations.length == 3)
  }

  "No category for inventory last week not sold " should "return false means should get filtered" in {
    val inventorySoldStatus = basicRecommender.inventoryWeekNotSold(null, 40, 20)
    assert(inventorySoldStatus == false)
  }

  "Stock less than weekly average " should "return false means should get filtered" in {
    val inventorySoldStatus = basicRecommender.inventoryWeekNotSold("SUNGLASSES", 20, 30)
    assert(inventorySoldStatus == false)
  }

  "Sunglasses category has stock more than double the weekly average " should "return true" in {
    val inventorySoldStatus = basicRecommender.inventoryWeekNotSold("SUNGLASSES", 50, 20)
    assert(inventorySoldStatus == true)
  }

  "Sunglasses category has stock less than double the weekly average " should "return false" in {
    val inventorySoldStatus = basicRecommender.inventoryWeekNotSold("SUNGLASSES", 35, 20)
    assert(inventorySoldStatus == false)
  }

  "Given a row and array of keys" should "create a dynamic row with those keys" in {
    val keys = Array(ProductVariables.BRICK, ProductVariables.MVP)
    val expectedRow: Seq[Any] = Seq("test", 1)
    val row = basicRecommender.createKey(basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame, 10), itrDataFrame).head(), keys)
    assert((row.toSeq).equals(expectedRow))
  }

  "Given a null row and array of keys" should "return null row" in {
    val keys = Array(ProductVariables.BRICK, ProductVariables.MVP)
    val row = basicRecommender.createKey(null, keys)
    assert(row == null)
  }

  "Given a  row and  no keys" should "return null row" in {
    val keys: Array[String] = Array()
    val row = basicRecommender.createKey(basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame, 10), itrDataFrame).head(), keys)
    assert(row == null)
  }

  "Given a row and array of keys with one Bad key" should "null row" in {
    val keys = Array(ProductVariables.BRICK, "Bad")
    val expectedRow: Seq[Any] = Seq("test", 1)
    val row = basicRecommender.createKey(basicRecommender.skuCompleteData(basicRecommender.topProductsSold(orderItemDataFrame, 10), itrDataFrame).head(), keys)
    assert(row == null)
  }

}
