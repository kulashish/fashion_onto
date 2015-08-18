package com.jabong.dap.campaign.customerselection

import java.io.File

import com.jabong.dap.common.{TestConstants, SharedSparkContext}
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 24/7/15.
 */
class YesterdaySessionTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerSurfData: DataFrame = _
  @transient var dfItrData: DataFrame = _

  var yesterdaySession: YesterdaySession = _

  override def beforeAll() {

    super.beforeAll()
    yesterdaySession = new YesterdaySession()
    dfCustomerSurfData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.CUSTOMER_SELECTION, TestConstants.CUSTOMER_PAGE_VISIT, Schema.customerPageVisitSkuListLevel)
    dfItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.CUSTOMER_SELECTION, TestConstants.ITR_30_DAY_DATA, Schema.itr)

  }

  //=======surf-1==========================================================
  "yesterdaySession(x): Data Frame yesterdaySession" should "null" in {

    val result = yesterdaySession.customerSelection(null)

    assert(result == null)

  }

  "yesterdaySession(x): count " should "3" in {

    val result = yesterdaySession.customerSelection(dfCustomerSurfData)

    assert(result.count() == 4)
  }

  //=======Surf-2==========================================================
  "yesterdaySession(x, y): Data Frame yesterdaySession" should "null" in {

    val result = yesterdaySession.customerSelection(null, null)

    assert(result == null)

  }

  "yesterdaySession(x, y): count " should "3" in {

    val result = yesterdaySession.customerSelection(dfCustomerSurfData, dfItrData)

    assert(result.count() == 3)
  }

}
