package com.jabong.dap.campaign.recommendation.generate

import com.jabong.dap.campaign.recommendation.generator.RecommendationUtils
import com.jabong.dap.common.json.JsonUtils
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec

/**
 * Created by rahul aneja on 31/8/15.
 */
class RecommendationUtilsTest extends FlatSpec {

  "No category  but stock is less than weekly average " should "return false means should  be removed" in {
    val inventorySoldStatus = RecommendationUtils.inventoryFilter(null, 5, 20, 40.0)
    assert(inventorySoldStatus == false)
  }

  "No category  but stock is equal than weekly average " should "return true means should not be filtered" in {
    val inventorySoldStatus = RecommendationUtils.inventoryFilter(null, 5, 40, 40.0)
    assert(inventorySoldStatus == true)
  }

  "Sunglasses category  and weekly average is null " should "return true means should not be filtered" in {
    val inventorySoldStatus = RecommendationUtils.inventoryFilter("SUNGLASSES", 5, 40, null)
    assert(inventorySoldStatus == true)
  }

  "Toys category  and weekly average is null " should "return false means should not be filtered" in {
    val inventorySoldStatus = RecommendationUtils.inventoryFilter("TOYS", 8, 10, null)
    assert(inventorySoldStatus == false)
  }

  "No gender value" should "return NO recommended gender" in {
    val recommendedGender = RecommendationUtils.getRecommendationGender(null)
    assert(recommendedGender == null)
  }

  "MEN gender value" should "return MEN,UNISEX recommended gender" in {
    val recommendedGender = RecommendationUtils.getRecommendationGender("MEN")
    assert(recommendedGender == "MEN!UNISEX")
  }

  "testFiloterCount" should "check if the filter on count works" in {
    val df = JsonUtils.readFromJson("campaigns/recommendation", "filter_data")
    val filtered =  df.filter(count(df("")).>(3))
    println(filtered)

  }

}
