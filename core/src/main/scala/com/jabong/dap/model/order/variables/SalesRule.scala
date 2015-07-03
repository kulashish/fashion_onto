package com.jabong.dap.model.order.variables

import com.jabong.dap.common.{ Spark, MergeUtils }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 26/6/15.
 */
object SalesRule {

  def getCode(salesRule: DataFrame, c: Int): DataFrame = {
    val filData = salesRule.filter(salesRule("code").startsWith("WC" + c + "0"))
    val wcCode = filData.select("fk_customer", "updated_at", "code", "created_at", "to_date")
    wcCode.printSchema()
    wcCode.show(1)
    println(wcCode.count())
    return wcCode
  }

  def createWcCodes(curr: String, prev: String) {
    val salesRulePath = DataSets.BOB_PATH + DataSets.SALES_RULE + "/" + curr
    val salesRule = Spark.getSqlContext().read.parquet(salesRulePath)
    val wc10 = getCode(salesRule, 1)
    val wc20 = getCode(salesRule, 2)
    wc10.write.parquet(DataSets.VARIABLE_PATH + "welcome1/" + curr)
    wc20.write.parquet(DataSets.VARIABLE_PATH + "welcome2/" + curr)
    val wc10Prev = Spark.getSqlContext().read.parquet(DataSets.VARIABLE_PATH + "welcome1/full/" + prev)
    val wc20Prev = Spark.getSqlContext().read.parquet(DataSets.VARIABLE_PATH + "welcome2/full/" + prev)
    val wc10Full = MergeUtils.InsertUpdateMerge(wc10Prev, wc10, "fk_customer")
    val wc20Full = MergeUtils.InsertUpdateMerge(wc10Prev, wc20, "fk_customer")
    wc10Full.write.parquet(DataSets.VARIABLE_PATH + "welcome1/full" + curr)
    wc20Full.write.parquet(DataSets.VARIABLE_PATH + "welcome2/full" + curr)
  }

}
