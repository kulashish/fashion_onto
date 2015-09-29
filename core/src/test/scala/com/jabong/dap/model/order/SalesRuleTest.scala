package com.jabong.dap.model.order

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.order.schema.OrderVarSchema
import com.jabong.dap.model.order.variables.SalesRule
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by jabong on 29/6/15.
 */
class SalesRuleTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    //    val config = new Config(basePath = "basePath")
    //    AppConfig.config = config

    //    df1 = sqlContext.read.json("sales_rule1.json")
    df1 = JsonUtils.readFromJson(DataSets.SALES_RULE, "sales_rule1", OrderVarSchema.salesRule)
    df1.collect.foreach(println)
  }

  "The result Dataframe" should "have size 4" in {
    val wcCodes = SalesRule.getCode(df1, "3")
    assert(wcCodes.count() == 6)
  }

  "The result Dataframe" should "have size 3" in {
    val wcCodes = SalesRule.getCode(df1, "5")
    assert(wcCodes.count == 3)
  }

  //  override def afterAll() {
  //    super.afterAll()
  //  }

}
