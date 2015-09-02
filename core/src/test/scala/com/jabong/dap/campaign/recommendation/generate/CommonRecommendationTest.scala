package com.jabong.dap.campaign.recommendation.generate

import com.jabong.dap.campaign.recommendation.generator.{ PivotRecommendation, CommonRecommendation }
import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.common.constants.variables.ProductVariables
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ NullInputException, SharedSparkContext, Spark, TestSchema }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ FlatSpec, Matchers }

import scala.collection.mutable

/**
 * Created by rahul aneja  on 28/8/15.
 */
class CommonRecommendationTest extends FlatSpec with SharedSparkContext with Matchers {

  @transient var sqlContext: SQLContext = _
  @transient var itrDataFrame: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _
  @transient var inventoryCheckInput: DataFrame = _
  @transient var skuCompleteInput: DataFrame = _
  var commonRecommendation: CommonRecommendation = _
  var days: Int = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    days = TimeUtils.daysFromToday(TimeUtils.getDate("2015-08-27", TimeConstants.DATE_FORMAT))
    commonRecommendation = PivotRecommendation
    orderItemDataFrame = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "sales_order_item_weekly_average_sales")
    inventoryCheckInput = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "inventory_check_input", TestSchema.inventoryCheckInput)
    skuCompleteInput = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "sku_complete_data_input", TestSchema.skuCompleteInput)
    itrDataFrame = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "basic_sku_itr")
  }

  "no order data frame Data Frame" should "return null data frame" in {
    val expectedValue = commonRecommendation.createWeeklyAverageSales(null)
    assert(expectedValue == null)
  }

  "last seven days order item dataframe" should "dataframe with sku SO596WA65JLIINDFAS and weekly average sale" in {
    val expectedValue = commonRecommendation.createWeeklyAverageSales(orderItemDataFrame)
    val expectedSku = expectedValue.filter(Recommendation.SALES_ORDER_ITEM_SKU + " = 'SO596WA65JLIINDFAS'")
    assert(expectedValue.count() == 1)
  }

  "last seven days order item dataframe" should "dataframe without sku ES418WA79UAUINDFAS weekly average sale" in {
    val expectedValue = commonRecommendation.createWeeklyAverageSales(orderItemDataFrame)
    val expectedSku = expectedValue.filter(Recommendation.SALES_ORDER_ITEM_SKU + " = 'ES418WA79UAUINDFAS'")
    assert(expectedSku.count() == 0)
  }

  "last seven days with weekly average sales and 30 days order item dataframe" should "adding weekly average 30 days " in {
    val expectedValue = commonRecommendation.addWeeklyAverageSales(commonRecommendation.createWeeklyAverageSales(orderItemDataFrame),
      commonRecommendation.topProductsSold(orderItemDataFrame))
    val expectedSku = expectedValue.filter(Recommendation.SALES_ORDER_ITEM_SKU + " = 'ES418WA79UAUINDFAS'")
    assert(expectedSku.count() == 1)
  }

  "null weekly average sales and 30 days order item dataframe" should " return NullInputException " in {
    a[NullInputException] should be thrownBy {
      commonRecommendation.addWeeklyAverageSales(null,
        commonRecommendation.topProductsSold(orderItemDataFrame))
    }
  }

  "null input dataframe in inventory check " should " return NullInputException " in {
    a[NullInputException] should be thrownBy {
      commonRecommendation.inventoryCheck(null)
    }
  }

  "input dataframe in inventory check " should " return skus with desired inventory" in {
    val expectedValue = commonRecommendation.inventoryCheck(inventoryCheckInput)
    val expectedSku = expectedValue.filter(Recommendation.SALES_ORDER_ITEM_SKU + " = 'ES418WA79UAUINDFAS'")
    assert(expectedSku.count() == 1)
  }

  "Null order data" should "return null dataframe" in {
    a[NullInputException] should be thrownBy {
      commonRecommendation.topProductsSold(null)
    }
  }

  "Order Items data input  " should "return sku and sorted by quantity dataframe" in {
    val topSkus = commonRecommendation.topProductsSold(orderItemDataFrame)
    assert(topSkus.count() == 2)
  }

  "top skus  input and itr" should "return sku complete data" in {
    val recInput = commonRecommendation.skuCompleteData(skuCompleteInput, itrDataFrame)
    val recInputBrand = recInput.filter(Recommendation.SALES_ORDER_ITEM_SKU + "='ES418WA79UAUINDFAS'").select(ProductVariables.BRAND).collect()(0)(0).toString()
    assert(recInputBrand == "adidas")
  }

  " skus BR828MA28TMPINDFAS input and itr" should "return WOMEN GENDER" in {
    val recInput = commonRecommendation.skuCompleteData(skuCompleteInput, itrDataFrame)
    val recInputBrand = recInput.filter(Recommendation.SALES_ORDER_ITEM_SKU + "='SO596WA65JLIINDFAS'").select(ProductVariables.GENDER).collect()(0)(0).toString()
    assert(recInputBrand == "WOMEN")
  }

  "No top skus input and itr" should "return IllegalArgumentException " in {
    a[IllegalArgumentException] should be thrownBy {
      commonRecommendation.skuCompleteData(null, itrDataFrame)
    }
  }

  "No Recommendation input skus" should "return IllegalArgumentException" in {
    a[IllegalArgumentException] should be thrownBy {
      commonRecommendation.genRecommend(null, null, null, 0)
    }
  }

  "5 recommendation input skus and pivot keys is null" should " return IllegalArgumentException" in {
    val dataFrameSchema = StructType(Array(
      StructField(ProductVariables.BRICK, StringType, false),
      StructField(ProductVariables.MVP, LongType, false),
      StructField(ProductVariables.GENDER, StringType, false),
      StructField(ProductVariables.RECOMMENDATIONS, ArrayType(StructType(Array(StructField(ProductVariables.QUANTITY, LongType), StructField(ProductVariables.SKU_LIST, StringType))), true))
    ))
    a[IllegalArgumentException] should be thrownBy {
      commonRecommendation.genRecommend(inventoryCheckInput, null, dataFrameSchema, 8)
    }
  }

  "5 recommendation input skus" should "create recommendation based on brick mvp and gender" in {
    val dataFrameSchema = StructType(Array(
      StructField(ProductVariables.BRICK, StringType, false),
      StructField(ProductVariables.MVP, LongType, false),
      StructField(ProductVariables.GENDER, StringType, false),
      StructField(ProductVariables.RECOMMENDATIONS, ArrayType(StructType(Array(StructField(ProductVariables.QUANTITY, LongType), StructField(ProductVariables.SKU_LIST, StringType))), true))
    ))
    val pivotKeys = Array(ProductVariables.BRICK, ProductVariables.MVP)

    val recOut = commonRecommendation.genRecommend(inventoryCheckInput, pivotKeys, dataFrameSchema, 8)

    val recommendations = recOut.filter(ProductVariables.GENDER + "='WOMEN'")
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

    val recOut = commonRecommendation.genRecommend(inventoryCheckInput, pivotKeys, dataFrameSchema, 8)

    val recommendations = recOut.filter(ProductVariables.GENDER + "='UNISEX'")
      .select(ProductVariables.RECOMMENDATIONS).collect()(0)(0).asInstanceOf[mutable.MutableList[(Long, String)]]
    assert(recommendations.length == 1)
  }

  "Given a row and array of keys" should "create a dynamic row with those keys" in {
    val keys = Array(ProductVariables.BRICK, ProductVariables.MVP)
    val expectedRow: Seq[Any] = Seq("test", "mass")
    val row = commonRecommendation.createKey(inventoryCheckInput.head(), keys)
    row.toSeq.foreach(println)
    assert((row.toSeq).equals(expectedRow))
  }

  "Given a null row and array of keys" should "return null row" in {
    val keys = Array(ProductVariables.BRICK, ProductVariables.MVP)
    val row = commonRecommendation.createKey(null, keys)
    assert(row == null)
  }

  "Given a  row and  no keys" should "return null row" in {
    val keys: Array[String] = Array()
    val row = commonRecommendation.createKey(inventoryCheckInput.head(), keys)
    assert(row == null)
  }

  "Given a row and array of keys with one Bad key" should "null row" in {
    val keys = Array(ProductVariables.BRICK, "Bad")
    val expectedRow: Seq[Any] = Seq("test", "mass")
    val row = commonRecommendation.createKey(inventoryCheckInput.head(), keys)
    assert(row == null)
  }

}

