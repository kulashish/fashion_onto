package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.schema.CustVarSchema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 13/7/15.
 */
class WishListTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerProductShortlist: DataFrame = _

  var wishlist: WishList = _

  override def beforeAll() {

    super.beforeAll()
    wishlist = new WishList()
    //    JsonUtils.writeToJson("/home/raghu/bigData/parquetFiles/", "customer_product_shortlist")
    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.CUSTOMER_PRODUCT_SHORTLIST, DataSets.CUSTOMER_PRODUCT_SHORTLIST, Schema.customerProductShortlist)

  }

  "customerSelection: Data Frame dfCustomerProductShortlist" should "null" in {

    val result = wishlist.customerSelection(null, 0)

    assert(result == null)

  }

  "customerSelection: ndays" should "negetive value" in {

    val result = wishlist.customerSelection(dfCustomerProductShortlist, -1)

    assert(result == null)

  }

  "customerSelection: schema attributes and data type" should "match into DataFrames(dfCustomerProductShortlist)" in {

    val result = wishlist.customerSelection(dfCustomerProductShortlist, 1)

    assert(result != null)

  }

  "customerSelection: Data Frame" should "match to resultant Data Frame, If dfFull is NULL" in {

    val result = wishlist.customerSelection(dfCustomerProductShortlist, 5)
      .limit(30).collect().toSet

    //                              result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_customer_wishlist" + ".json")

    val dfCustomerProductShortlistResult = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.CUSTOMER_PRODUCT_SHORTLIST, DataSets.RESULT_CUSTOMER_WISHLIST, Schema.resultCustomerWishlist)
      .collect().toSet

    assert(result.equals(dfCustomerProductShortlistResult))
  }

}
