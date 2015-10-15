package com.jabong.dap.model.responsys.campaigndata

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.campaigndata.CustomerPreferredTimeslotPart2
import com.jabong.dap.model.customer.schema.CustVarSchema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 13/10/15.
 */
class CustomerPreferredTimeslotPart1Test extends FlatSpec with SharedSparkContext {

  @transient var dfEmailOpen: DataFrame = _
  @transient var dfEmailClick: DataFrame = _

  override def beforeAll() {

    super.beforeAll()
    dfEmailOpen = JsonUtils.readFromJson(DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, "email_open")
    dfEmailClick = JsonUtils.readFromJson(DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, "email_click")
  }

  "getCPOTPart1: Data Frame" should "match to resultant Data Frame" in {

  }

}
