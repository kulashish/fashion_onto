package com.jabong.dap.model.responsys.campaigndata

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.customer.schema.CustVarSchema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

import scala.collection.mutable.HashMap

/**
 * Created by raghu on 13/10/15.
 */
class CustomerPreferredTimeslotPart1Test extends FlatSpec with SharedSparkContext {

  @transient var dfEmailOpen: DataFrame = _
  @transient var dfEmailClick: DataFrame = _
  @transient var dfFullCPOTPart1: DataFrame = _

  override def beforeAll() {

    super.beforeAll()
    dfEmailOpen = JsonUtils.readFromJson(DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, "email_open")
    dfEmailClick = JsonUtils.readFromJson(DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, "email_click")
    dfFullCPOTPart1 = JsonUtils.readFromJson(DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, "cpotPart1", CustVarSchema.customersPreferredOrderTimeslotPart1)

  }

  "getCPOTPart1: Data Frame count" should "10" in {
    val dfMap = new HashMap[String, DataFrame]()
    dfMap.put("openIncr", dfEmailOpen)
    dfMap.put("clickIncr", dfEmailClick)
    dfMap.put("cpotPart1PrevFull", null)

    val dfWrite = CustomerPreferredTimeslotPart1.process(dfMap)

    assert(dfWrite("dfCPOTPart1Incr").count() == 10)
    assert(dfWrite("dfCPOTPart1Full").count() == 10)

  }
  //
  //  "getCPOTPart1: Data Frame" should "match to resultant Data Frame" in {
  //    val dfMap = new HashMap[String, DataFrame]()
  //    dfMap.put("openIncr", dfEmailOpen)
  //    dfMap.put("clickIncr", dfEmailClick)
  //    dfMap.put("cpotPart1PrevFull", dfFullCPOTPart1)
  //
  //    val dfWrite = CustomerPreferredTimeslotPart1.process(dfMap)
  //
  //    assert(dfWrite("dfCPOTPart1Incr").count() == 10)
  //    assert(dfWrite("dfCPOTPart1Full").count() == 10)
  //
  //  }

}
