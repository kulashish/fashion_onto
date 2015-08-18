package com.jabong.dap.model.customer

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.schema.CustVarSchema
import com.jabong.dap.model.customer.variables.{Customer, CustomerSegments, CustomerStorecreditsHistory}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FlatSpec

/**
 * Created by raghu on 16/6/15.
 */
class CustomerTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomer: DataFrame = _
  @transient var dfNLS: DataFrame = _
  @transient var dfSalesOrder: DataFrame = _
  @transient var dfCSH: DataFrame = _
  @transient var dfCustomerSegments: DataFrame = _

  override def beforeAll() {

    super.beforeAll()

    dfCustomer = JsonUtils.readFromJson(DataSets.CUSTOMER, DataSets.CUSTOMER, Schema.customer)
    dfNLS = JsonUtils.readFromJson(DataSets.NEWSLETTER_SUBSCRIPTION, DataSets.NEWSLETTER_SUBSCRIPTION, Schema.nls)
    dfSalesOrder = JsonUtils.readFromJson(DataSets.SALES_ORDER, DataSets.SALES_ORDER, Schema.salesOrder)
    dfCSH = JsonUtils.readFromJson(DataSets.CUSTOMER_STORECREDITS_HISTORY, DataSets.CUSTOMER_STORECREDITS_HISTORY, Schema.csh)
    dfCustomerSegments = JsonUtils.readFromJson(DataSets.CUSTOMER_SEGMENTS, DataSets.CUSTOMER_SEGMENTS, Schema.customerSegments)

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
      .collect().toSet

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

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: EMAIL_OPT_IN_STATUS
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getEmailOptInStatus: getStatusValue " should "O" in {

    val result = Customer.getEmailOptInStatus(null, null)

    assert(result == "O")

  }

  "getEmailOptInStatus: getStatusValue " should "I" in {

    val row = Row("", "subscribed")

    val result = Customer.getEmailOptInStatus("1", "subscribed")

    assert(result == "I")

  }

  "getEmailOptInStatus: getStatusValue " should "U" in {

    val row = Row("", "unsubscribed")

    val result = Customer.getEmailOptInStatus("1", "unsubscribed")

    assert(result == "U")

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: id_customer, CUSTOMERS PREFERRED ORDER TIMESLOT
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getCustomersPreferredOrderTimeslot: Data Frame" should "match to resultant Data Frame" in {

    val result = Customer.getCPOT(dfSalesOrder: DataFrame)
      .limit(30).collect().toSet

    //            result.limit(30).write.json(DataSets.TEST_RESOURCES + "customers_preferred_order_timeslot" + ".json")

    val dfCustomersPreferredOrderTimeslot = JsonUtils.readFromJson(DataSets.CUSTOMER, "customers_preferred_order_timeslot",
      CustVarSchema.customersPreferredOrderTimeslot)
      .collect().toSet

    assert(result.equals(dfCustomersPreferredOrderTimeslot) == true)

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
      CustVarSchema.last_jr_covert_date)
      .collect().toSet

    assert(result.equals(dfLastJrCovertDate) == true)

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: fk_customer, MVP, Segment0, Segment1,Segment2, Segment3, Segment4, Segment5, Segment6
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getMvpAndSeg: Data Frame dfCustomerSegments" should "null" in {

    val result = CustomerSegments.getMvpAndSeg(null)

    assert(result == null)

  }

  "getMvpAndSeg: schema attributes and data type" should
    "match into DataFrame(dfCSH)" in {

      val result = CustomerSegments.getMvpAndSeg(dfCustomerSegments: DataFrame)

      assert(result != null)

    }

  "getMvpAndSeg: Data Frame" should "match to resultant Data Frame" in {

    val result = CustomerSegments.getMvpAndSeg(dfCustomerSegments: DataFrame)
      .limit(30).collect().toSet

    //                        result.limit(30).write.json(DataSets.TEST_RESOURCES + "mvp_seg" + ".json")

    val dfMvpSeg = JsonUtils.readFromJson(DataSets.CUSTOMER, "mvp_seg", CustVarSchema.mvp_seg)
      .collect().toSet

    assert(result.equals(dfMvpSeg) == true)

  }

  //  override def afterAll() {
  //    super.afterAll()
  //  }

}
