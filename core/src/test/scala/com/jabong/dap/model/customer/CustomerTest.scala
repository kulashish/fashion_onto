package com.jabong.dap.model.customer

import com.jabong.dap.common.SharedSparkContext
import org.scalatest.FlatSpec

/**
 * Created by raghu on 16/6/15.
 */
class CustomerTest extends FlatSpec with SharedSparkContext {

  //  @transient var dfCustomer: DataFrame = _
  //  @transient var dfNLS: DataFrame = _
  //  @transient var dfSalesOrder: DataFrame = _

  override def beforeAll() {

    super.beforeAll()

    // dfCustomer = JsonUtils.readFromJson(DataSets.CUSTOMER, DataSets.CUSTOMER, Schema.customer)
    // dfNLS = JsonUtils.readFromJson(DataSets.NEWSLETTER_SUBSCRIPTION, DataSets.NEWSLETTER_SUBSCRIPTION, Schema.nls)
    // dfSalesOrder = JsonUtils.readFromJson(DataSets.SALES_ORDER, DataSets.SALES_ORDER, Schema.salesOrder)

  }

  /*////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // //Name of variable: EMAIL, ACC_REG_DATE, UPDATED_AT
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getCustomer: Data Frame dfCustomer, dfNLS, dfSalesOrder" should "null" in {

    val result = Customer.getCustomer(null, null, null, null)

    assert(result == null)

  }

  "getCustomer: schema attributes and data type" should
    "match into DataFrames(dfCustomer, dfNLS, dfSalesOrder)" in {

      val result = Customer.getCustomer(
        dfCustomer: DataFrame,
        dfNLS: DataFrame,
        dfSalesOrder: DataFrame, null
      )
      assert(result != null)

    }

  "getCustomer: Data Frame" should "match to resultant Data Frame, If dfFull is NULL" in {

    var dfFull: DataFrame = null

    val result = Customer.getCustomer(
      dfCustomer: DataFrame,
      dfNLS: DataFrame,
      dfSalesOrder: DataFrame, dfFull
    )
    //      .limit(30).collect().toSet

    //               result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_customer_new" + ".json")

    val dfResultCustomer = JsonUtils.readFromJson(DataSets.CUSTOMER, "result_customer_new",
      CustVarSchema.resultCustomer)
    dfResultCustomer.show()
    //.collect().toSet

    result._1.show()
    assert(result._1.limit(30).collect().toSet.equals(dfResultCustomer) == true)

    assert(result._2 == null)

  }

  "getCustomer: Data Frame" should "match to resultant Data Frame, If dfFull is Not NULL" in {

    var dfFull: DataFrame = JsonUtils.readFromJson(DataSets.CUSTOMER, "result_customer_old", CustVarSchema.resultCustomer)

    val result = Customer.getCustomer(
      dfCustomer: DataFrame,
      dfNLS: DataFrame,
      dfSalesOrder: DataFrame, dfFull: DataFrame
    )
    //      .limit(30).collect().toSet

    //    result._2.limit(30).write.json(DataSets.TEST_RESOURCES + "result_customer_incremental" + ".json")

    val dfResultNew = JsonUtils.readFromJson(DataSets.CUSTOMER, "result_customer_new",
      CustVarSchema.resultCustomer)
      .collect().toSet

    val dfResultIncremental = JsonUtils.readFromJson(DataSets.CUSTOMER, "result_customer_incremental",
      CustVarSchema.resultCustomer)
      .collect().toSet

    assert(result._1.limit(30).collect().toSet.equals(dfResultNew) == true)

    assert(result._2.limit(30).collect().toSet.equals(dfResultIncremental) == true)

  }*/

}
