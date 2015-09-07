package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 7/9/15.
 */
class NewArrivalsBrandTest extends FlatSpec with SharedSparkContext {
  @transient var salesCartData: DataFrame = _
  var newArrivalsBrand: NewArrivalsBrand = _

  override def beforeAll() {
    super.beforeAll()
    newArrivalsBrand = new NewArrivalsBrand()
    salesCartData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/new_arrivals_brand", "sales_cart")
  }

  "Null salesCartData DataFrame" should "return null" in {
    val customerSelected = newArrivalsBrand.customerSelection(null)
    assert(customerSelected == null)
  }

  "salesCartData DataFrame " should "return 2" in {
    val customerSelected = newArrivalsBrand.customerSelection(salesCartData)
    assert(customerSelected.count() == 2)
  }

}
