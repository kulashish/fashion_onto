package com.jabong.dap.model.customer

import com.jabong.dap.common.{TestSchema, SharedSparkContext}
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.variables.CustomerStorecreditsHistory
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by pooja on 30/9/15.
 */
class CustomerStoreCreditHistoryTest  extends FlatSpec with SharedSparkContext {

  @transient var dfCSH: DataFrame = _

  override def beforeAll() {

    super.beforeAll()

    dfCSH = JsonUtils.readFromJson(DataSets.CUSTOMER_STORECREDITS_HISTORY, DataSets.CUSTOMER_STORECREDITS_HISTORY, Schema.csh)

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: fk_customer, LAST_JR_COVERT_DATE
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getLastJrCovertDate: Data Frame dfCSH" should "null" in {

    val result = CustomerStorecreditsHistory.getLastJrCovertDate(null)

    assert(result == null)

  }

  "getLastJrCovertDate: schema attributes and data type" should
    "match into DataFrame(dfCSH)" in {

    val result = CustomerStorecreditsHistory.getLastJrCovertDate(dfCSH: DataFrame)

    assert(result != null)

  }

  "getLastJrCovertDate: Data Frame" should "match to resultant Data Frame" in {

    val result = CustomerStorecreditsHistory.getLastJrCovertDate(dfCSH: DataFrame)
      .limit(30).collect().toSet

    //                result.limit(30).write.json(DataSets.TEST_RESOURCES + "last_jr_covert_date" + ".json")

    val dfLastJrCovertDate = JsonUtils.readFromJson(DataSets.CUSTOMER, "last_jr_covert_date",
      TestSchema.last_jr_covert_date)
      .collect().toSet

    assert(result.equals(dfLastJrCovertDate) == true)

  }

  //  override def afterAll() {
  //    super.afterAll()
  //  }

}
