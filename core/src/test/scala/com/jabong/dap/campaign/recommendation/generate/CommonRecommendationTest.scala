package com.jabong.dap.campaign.recommendation.generate

import com.jabong.dap.campaign.recommendation.generator.CommonRecommendation
import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.{NullInputException, SharedSparkContext, Spark, TestSchema}
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by rahul aneja  on 28/8/15.
 */
class CommonRecommendationTest extends FlatSpec with SharedSparkContext with Matchers {

  @transient var sqlContext: SQLContext = _
  @transient var itrDataFrame: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _
  @transient var inventoryCheckInput: DataFrame = _
  var commonRecommendation: CommonRecommendation = _
  var days: Int = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    days = TimeUtils.daysFromToday(TimeUtils.getDate("2015-08-27", TimeConstants.DATE_FORMAT))
    commonRecommendation = new CommonRecommendation()
    orderItemDataFrame = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "sales_order_item_weekly_average_sales")
    inventoryCheckInput = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "inventory_check_input",TestSchema.inventoryCheckInput)
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


}

