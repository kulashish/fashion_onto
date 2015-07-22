package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables }
import com.jabong.dap.common.{ Spark, SharedSparkContext }
import org.apache.spark.sql.{ Row, DataFrame, SQLContext }
import org.scalatest.FlatSpec

/**
 * Created by rahul for com.jabong.dap.campaign.skuselection on 18/7/15.
 */
class FollowUpTest extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var customerSelected: DataFrame = _
  @transient var itrData: DataFrame = _
  var followUp: FollowUp = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    followUp = new FollowUp()
    customerSelected = sqlContext.read.json("src/test/resources/campaign/invalid_campaigns/invalid_followup_customer_select.json")
    itrData = sqlContext.read.json("src/test/resources/campaign/invalid_campaigns/itr_followup.json")
  }

  "empty customer selected data " should "return empty ref skus" in {
    val skuData = followUp.skuFilter(null, itrData)
    assert(skuData == null)
  }

  "empty itrData selected data " should "return empty ref skus" in {
    val skuData = followUp.skuFilter(customerSelected, null)
    assert(skuData == null)
  }

  "Invalid customer selected data and last days itr " should "ref skus of fk_customer" in {
    val refSkus = followUp.skuFilter(customerSelected, itrData)
    val refSkuList = refSkus.filter(CustomerVariables.FK_CUSTOMER + " = " + 8552648).select(ProductVariables.SKU_LIST).collect()(0)(0).asInstanceOf[List[(Double, String)]]
    refSkus.collect().foreach(println)
    val expectedData = Row(120.7, "ES418WA79UAUINDFAS-4540894")
    assert(refSkus.count() == 1)
    assert(refSkuList.head === expectedData)

  }

}
