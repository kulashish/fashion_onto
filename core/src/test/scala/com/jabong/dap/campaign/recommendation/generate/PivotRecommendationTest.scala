package com.jabong.dap.campaign.recommendation.generate

import com.jabong.dap.campaign.recommendation.generator.{PivotRecommendation, CommonRecommendation}
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark, TestSchema }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ FeatureSpec, GivenWhenThen }

/**
 * Created by rahul on 1/9/15.
 */
class PivotRecommendationTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var itrDataFrame: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _
  @transient var inventoryCheckInput: DataFrame = _
  var commonRecommendation: CommonRecommendation = _
  var incrDate: String = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    incrDate = "2015/08/27"
    orderItemDataFrame = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "sales_order_item_weekly_average_sales", Schema.salesOrderItem)
    itrDataFrame = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "basic_sku_itr", TestSchema.basicItr)
  }

  feature("Generate Pivot Recommendations"){
    scenario("when subtype is brick_mvp"){
      Given("order full item data and yesterday itr")
      val brickMvp = "brick_mvp"
      val brandMvp = "brand_mvp"
      When("subtype = brick_mvp")
      //PivotRecommendation.generateRecommendation(orderItemDataFrame,itrDataFrame,brickMvp,10,incrDate)
      When("subtype = brand_mvp")
      //PivotRecommendation.generateRecommendation(orderItemDataFrame,itrDataFrame,brandMvp,10,incrDate)
    }
  }
}
