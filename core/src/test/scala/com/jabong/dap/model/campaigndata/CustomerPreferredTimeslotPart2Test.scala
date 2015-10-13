package com.jabong.dap.model.campaigndata

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
class CustomerPreferredTimeslotPart2Test extends FlatSpec with SharedSparkContext {

  @transient var dfSalesOrder: DataFrame = _

  override def beforeAll() {

    super.beforeAll()
    dfSalesOrder = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order", Schema.salesOrder)
  }

  "getCPOT: Data Frame" should "match to resultant Data Frame" in {

    val result = CustomerPreferredTimeslotPart2.getCPOT(dfSalesOrder: DataFrame)
      .limit(30).collect().toSet

    //result.limit(30).write.json(DataSets.TEST_RESOURCES + "customers_preferred_order_timeslot" + ".json")

    val dfCustomersPreferredOrderTimeslot = JsonUtils.readFromJson(DataSets.CUSTOMER, "customers_preferred_order_timeslot",
      CustVarSchema.customersPreferredOrderTimeslotPart2)
      .collect().toSet

    //    result.collect().foreach(println)
    //    result.printSchema()
    //
    //    dfCustomersPreferredOrderTimeslot.collect().foreach(println)
    //    dfCustomersPreferredOrderTimeslot.printSchema()

    assert(result.equals(dfCustomersPreferredOrderTimeslot) == true)

  }

  "getCPOTPart2: Data Frame" should "match to resultant Data Frame" in {

    val dfCPOTFull = JsonUtils.readFromJson(DataSets.CUSTOMER, "customers_preferred_order_timeslot",
      CustVarSchema.customersPreferredOrderTimeslotPart2)

    val (dfInc, dfFullFinal) = CustomerPreferredTimeslotPart2.getCPOTPart2(dfSalesOrder, dfCPOTFull)

    assert(dfInc.count() == 5)
    assert(dfFullFinal.count() == 5)

  }

}
