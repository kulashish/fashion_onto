package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.schema.CustVarSchema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

import scala.collection.mutable.HashMap

/**
 * Created by raghu on 13/10/15.
 */
class CustomerPreferredTimeslotPart2Test extends FlatSpec with SharedSparkContext {

  @transient var dfSalesOrder: DataFrame = _
  @transient var dfCmrFull: DataFrame = _
  @transient var dfFullCPOTPart2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    dfSalesOrder = JsonUtils.readFromJson(DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, "sales_order", Schema.salesOrder)
    dfCmrFull = JsonUtils.readFromJson(DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, "cmr")
    dfFullCPOTPart2 = JsonUtils.readFromJson(DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, "cpotPart2", CustVarSchema.customersPreferredOrderTimeslotPart2)

  }

  "getCPOTPart2: dfFullCPOTPart2" should "null" in {

    val dfCPOTFull = JsonUtils.readFromJson(DataSets.CUSTOMER, "customers_preferred_order_timeslot", CustVarSchema.customersPreferredOrderTimeslotPart2)
    val dfMap = new HashMap[String, DataFrame]()
    dfMap.put("salesOrderIncr", dfSalesOrder)
    dfMap.put("cmrFull", dfCmrFull)

    val dfWrite = CustomerPreferredTimeslotPart2.process(dfMap)

    //        dfInc.collect().foreach(println)
    //        dfInc.printSchema()
    //
    //        dfFullFinal.collect().foreach(println)
    //        dfFullFinal.printSchema()

    assert(dfWrite("CPOTPart2Incr").count() == 5)
    assert(dfWrite("CPOTPart2Full") == 5)

  }

  "getCPOTPart2: dfFullFinal" should "6" in {
    val dfMap = new HashMap[String, DataFrame]()
    dfMap.put("salesOrderIncr", dfSalesOrder)
    dfMap.put("cmrFull", dfCmrFull)
    dfMap.put("CPOTPart2PrevFull", dfFullCPOTPart2)

    val dfWrite = CustomerPreferredTimeslotPart2.process(dfMap)

    //        dfInc.collect().foreach(println)
    //        dfInc.printSchema()
    //
    //        dfFullFinal.collect().foreach(println)
    //        dfFullFinal.printSchema()

    assert(dfWrite("CPOTPart2Incr").count() == 1)
    assert(dfWrite("CPOTPart2Full").count() == 6)

  }

}
