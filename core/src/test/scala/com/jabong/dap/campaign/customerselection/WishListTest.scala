package com.jabong.dap.campaign.customerselection

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.jabong.dap.common.{ TestSchema, TestConstants, SharedSparkContext }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
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
    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + DataSets.CUSTOMER_PRODUCT_SHORTLIST, DataSets.CUSTOMER_PRODUCT_SHORTLIST, Schema.customerProductShortlist)

  }

  "customerSelection: Data Frame dfCustomerProductShortlist" should "null" in {

    val result = wishlist.customerSelection(null)

    assert(result == null)

  }

  //  "customerSelection: ndays" should "negetive value" in {
  //
  //    val result = wishlist.customerSelection(dfCustomerProductShortlist)
  //
  //    assert(result == null)
  //
  //  }

  "customerSelection: schema attributes and data type" should "match into DataFrames(dfCustomerProductShortlist)" in {

    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)

    val date = format.parse("2015-07-09 00:00:08.0")

    val ndays = TimeUtils.daysFromToday(date)

    val result = wishlist.customerSelection(dfCustomerProductShortlist)

    assert(result != null)

  }

  //  "customerSelection: Data Frame" should "match to resultant Data Frame, If dfFull is NULL" in {
  //
  //    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
  //
  //    val date = format.parse("2015-07-09 00:00:08.0")
  //
  //    val ndays = TimeUtils.daysFromToday(date)
  //
  //    val result = wishlist.customerSelection(dfCustomerProductShortlist)
  //      .limit(30).collect().toSet
  //
  //    val dfCustomerProductShortlistResult = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + DataSets.CUSTOMER_PRODUCT_SHORTLIST, TestConstants.RESULT_CUSTOMER_PRODUCT_SHORTLIST, TestSchema.resultCustomerProductShortlist)
  //      .limit(30).collect().toSet
  //
  //    assert(result.equals(dfCustomerProductShortlistResult))
  //  }

  //  "customerSelection: Timestamp" should "Today" in {
  //
  //    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
  //
  //    val date = TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT)
  //
  //    val ndays = TimeUtils.daysFromToday(Timestamp.valueOf(date))
  //
  //    val result = wishlist.customerSelection(dfCustomerProductShortlist)
  //
  //    assert(result == null)
  //
  //  }

  "customerSelection:result length" should "8" in {

    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)

    val date = format.parse("2015-07-10 00:00:00.0")

    val ndays = TimeUtils.daysFromToday(date)

    val result = wishlist.customerSelection(dfCustomerProductShortlist)
    //      .limit(30).collect().toSet

    //                                  result.limit(30).write.json(DataSets.TEST_RESOURCES + DataSets.RESULT_CUSTOMER_PRODUCT_SHORTLIST + ".json")

    assert(result.count() == 8)
  }

}