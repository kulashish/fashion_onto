package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

import scala.collection.mutable.HashMap

/**
 * Created by raghu on 27/10/15.
 */
class PaybackDataTest extends FlatSpec with SharedSparkContext {

  @transient var salesOrder: DataFrame = _
  @transient var paymentPrepaidTransactionData: DataFrame = _
  @transient var paymentBankPriority: DataFrame = _
  @transient var soPaybackEarn: DataFrame = _
  @transient var soPaybackRedeem: DataFrame = _
  @transient var dfCmrFull: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    salesOrder = JsonUtils.readFromJson(DataSets.PAYBACK_DATA, "sales_order")
    paymentPrepaidTransactionData = JsonUtils.readFromJson(DataSets.PAYBACK_DATA, "payment_prepaid_transaction_data")
    paymentBankPriority = JsonUtils.readFromJson(DataSets.PAYBACK_DATA, "payment_bank_priority")
    soPaybackEarn = JsonUtils.readFromJson(DataSets.PAYBACK_DATA, "sales_order_payback_earn")
    soPaybackRedeem = JsonUtils.readFromJson(DataSets.PAYBACK_DATA, "sales_order_payback_redeem")
    dfCmrFull = JsonUtils.readFromJson(DataSets.PAYBACK_DATA, "sales_order_payback_redeem")
    dfCmrFull = JsonUtils.readFromJson(DataSets.CUSTOMER, "cmr")

  }

  "getPaybackData: Data Frame size" should "1" in {
    val dfMap = new HashMap[String, DataFrame]()

    dfMap.put("paymentBankPriorityFull", paymentBankPriority)
    dfMap.put("cmrFull", dfCmrFull)
    dfMap.put("paybackDataPrevFull", null)
    dfMap.put("salesOrderIncr", salesOrder)
    dfMap.put("paymentPrepaidTransactionDataIncr", paymentPrepaidTransactionData)
    dfMap.put("paybackEarnIncr", soPaybackEarn)
    dfMap.put("paybackRedeemIncr", soPaybackRedeem)

    val dfWrite = PaybackData.process(dfMap)

    //    dfInc.collect().foreach(println)
    //    dfInc.printSchema()
    //
    //    dfFull.collect().foreach(println)
    //    dfFull.printSchema()

    assert(dfWrite("paybackIncr").count() == 1)
    assert(dfWrite("paybackDataFull").count() == 1)
  }

}