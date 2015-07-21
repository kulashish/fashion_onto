package com.jabong.dap.campaign.customerselection

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.{ Constants, TimeUtils }
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

    val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)

    val date = format.parse("2015-07-09 00:00:08.0")

    val ndays = TimeUtils.daysFromToday(date).toInt

    val result = wishlist.customerSelection(dfCustomerProductShortlist, ndays)

    assert(result != null)

  }

  "customerSelection: Data Frame" should "match to resultant Data Frame, If dfFull is NULL" in {

    val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)

    val date = format.parse("2015-07-09 00:00:08.0")

    val ndays = TimeUtils.daysFromToday(date).toInt

    val result = wishlist.customerSelection(dfCustomerProductShortlist, ndays)
      .limit(30).collect().toSet

    //                                      result.limit(30).write.json(DataSets.TEST_RESOURCES + DataSets.RESULT_CUSTOMER_PRODUCT_SHORTLIST + ".json")

    val dfCustomerProductShortlistResult = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.CUSTOMER_PRODUCT_SHORTLIST, DataSets.RESULT_CUSTOMER_PRODUCT_SHORTLIST, Schema.resultCustomerProductShortlist)
      .limit(30).collect().toSet

    assert(result.equals(dfCustomerProductShortlistResult))
  }

  "customerSelection: Timestamp" should "Today" in {

    val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)

    val date = TimeUtils.getTodayDate(Constants.DATE_TIME_FORMAT)

    val ndays = TimeUtils.daysFromToday(Timestamp.valueOf(date)).toInt

    val result = wishlist.customerSelection(dfCustomerProductShortlist, ndays)

    assert(result == null)

  }

  "customerSelection:result length" should "8" in {

    val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)

    val date = format.parse("2015-07-10 00:00:00.0")

    val ndays = TimeUtils.daysFromToday(date).toInt

    val result = wishlist.customerSelection(dfCustomerProductShortlist, ndays)
    //      .limit(30).collect().toSet

    //                                  result.limit(30).write.json(DataSets.TEST_RESOURCES + DataSets.RESULT_CUSTOMER_PRODUCT_SHORTLIST + ".json")

    assert(result.count() == 8)
  }

}