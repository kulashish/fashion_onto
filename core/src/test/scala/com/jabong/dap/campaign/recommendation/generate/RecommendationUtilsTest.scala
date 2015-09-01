package com.jabong.dap.campaign.recommendation.generate

import com.jabong.dap.campaign.recommendation.generator.RecommendationUtils
import org.scalatest.FlatSpec

/**
 * Created by rahul aneja on 31/8/15.
 */
class RecommendationUtilsTest extends FlatSpec {

  "No category  but stock is less than weekly average " should "return false means should  be removed" in {
    val inventorySoldStatus = RecommendationUtils.inventoryFilter(null, 5, 20, 40)
    assert(inventorySoldStatus == false)
  }

  "No category  but stock is equal than weekly average " should "return true means should not be filtered" in {
    val inventorySoldStatus = RecommendationUtils.inventoryFilter(null, 5, 40, 40)
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

}
