package com.jabong.dap.model.responses.campaigndata

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

  "getCPOTPart2: Data Frame" should "match to resultant Data Frame" in {

    val dfCPOTFull = JsonUtils.readFromJson(DataSets.CUSTOMER, "customers_preferred_order_timeslot",
      CustVarSchema.customersPreferredOrderTimeslotPart2)

    val (dfInc, dfFullFinal) = CustomerPreferredTimeslotPart2.getCPOTPart2(dfSalesOrder, dfCPOTFull)

    //    dfInc.collect().foreach(println)
    //    dfInc.printSchema()
    //
    //    dfFullFinal.collect().foreach(println)
    //    dfFullFinal.printSchema()

    assert(dfInc.count() == 5)
    assert(dfFullFinal.count() == 5)

  }

}
