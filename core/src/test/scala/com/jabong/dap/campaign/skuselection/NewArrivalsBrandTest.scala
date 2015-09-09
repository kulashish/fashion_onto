package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 7/9/15.
 */
class NewArrivalsBrandTest extends FlatSpec with SharedSparkContext {
  @transient var itrData: DataFrame = _
  @transient var customerSelected: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    itrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/new_arrivals_brand", "itr")
    customerSelected = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/new_arrivals_brand", "sales_cart")
  }

  "Null itrData DataFrame" should "return null" in {
    val dfFilteredSku = NewArrivalsBrand.skuFilter(null, null)
    assert(dfFilteredSku == null)
  }

  "itrData DataFrame " should "return 0" in {
    val dfFilteredSku = NewArrivalsBrand.skuFilter(customerSelected, itrData)
    assert(dfFilteredSku.count() == 0)
  }

}
