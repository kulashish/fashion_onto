package com.jabong.dap.model.customer

import com.jabong.dap.common.{TestConstants, SharedSparkContext}
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.variables.PaybackCustomer
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by Kapil.Rajak on 7/7/15.
 */
class PaybackCustomerTest extends FlatSpec with SharedSparkContext {

  @transient var dfSalesOrder: DataFrame = _
  @transient var dfPaybackEarn: DataFrame = _
  @transient var dfPaybackRedeem: DataFrame = _
  @transient var expectedPaybackCustomers: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dfSalesOrder = JsonUtils.readFromJson(TestConstants.PAYBACK_CUSTOMER, DataSets.SALES_ORDER, Schema.salesOrder)
    dfPaybackEarn = JsonUtils.readFromJson(TestConstants.PAYBACK_CUSTOMER, DataSets.SALES_ORDER_PAYBACK_EARN, Schema.salesOrderPaybackEarn)
    dfPaybackRedeem = JsonUtils.readFromJson(TestConstants.PAYBACK_CUSTOMER, DataSets.SALES_ORDER_PAYBACK_REDEEM, Schema.salesOrderPaybackRedeem)
    expectedPaybackCustomers = JsonUtils.readFromJson(TestConstants.PAYBACK_CUSTOMER, TestConstants.PAYBACK_CUSTOMER, Schema.paybackCustomer)
  }

  "getPaybackCustomer: Data Frame" should "match with expected output" in {
    val dfPaybackCustomerResult = PaybackCustomer.getIncrementalPaybackCustomer(dfSalesOrder, dfPaybackEarn, dfPaybackRedeem)
    //dfPaybackCustomerResult.limit(30).write.json(DataSets.TEST_RESOURCES + DataSets.PAYBACK_CUSTOMER + ".json")
    assert(dfPaybackCustomerResult.collect().toSet.equals(expectedPaybackCustomers.collect().toSet))
  }

}
