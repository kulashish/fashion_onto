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

  @transient var dfCustomerWishlist: DataFrame = _

  var wishlist: WishList = _

  override def beforeAll() {

    super.beforeAll()
    wishlist = new WishList()
    //    dfCustomerWishlist = JsonUtils.readFromJson(DataSets.CUSTOMER_WISHLIST, DataSets.CUSTOMER_WISHLIST, Schema.customerWishlist)

  }

  "customerSelection: Data Frame dfCustomerWishlist" should "null" in {

    val result = wishlist.customerSelection(null, 0)

    assert(result == null)

  }

  //  "customerSelection: schema attributes and data type" should "match into DataFrames(dfCustomerWishlist)" in {
  //
  //    val result = wishlist.customerSelection(dfCustomerWishlist, 0)
  //
  //    assert(result != null)
  //
  //  }

  //  "customerSelection: Data Frame" should "match to resultant Data Frame, If dfFull is NULL" in {
  //
  //    val result = wishlist.customerSelection(dfCustomerWishlist, 5)
  ////      .limit(30).collect().toSet
  //
  //                            result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_customer_wishlist" + ".json")
  //
  ////    val dfCustomerWishlistResult = JsonUtils.readFromJson(DataSets.CUSTOMER_WISHLIST, "result_customer_wishlist", Schema.customerWishlist)
  ////      .collect().toSet
  ////
  ////    assert(result.equals(dfCustomerWishlistResult))
  //  }

}
