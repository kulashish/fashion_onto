package com.jabong.dap.model.order.variables

import com.jabong.dap.common.constants.variables.SalesRuleVariables
import com.jabong.dap.common.Spark
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * Created by mubarak on 26/6/15.
 * 
 */
object SalesRule {

  /**
   *
   * @param salesRule DataFrame with the sales_rule data
   * @param c int to filter the welcome code
   * @return DataFrame with the welcome codes
   */
  def getCode(salesRule : DataFrame, c :Int):DataFrame={
    val filData = salesRule.filter(salesRule(SalesRuleVariables.CODE).startsWith("WC"+c+"0"))
    val wcCode = filData.select(SalesRuleVariables.FK_CUSTOMER, SalesRuleVariables.UPDATED_AT, SalesRuleVariables.CODE, SalesRuleVariables.CREATED_AT, SalesRuleVariables.TO_DATE)
    wcCode.printSchema()
    wcCode.show(1)
    println(wcCode.count())
    wcCode
  }

  /**
   *
   * @param salesRule sales_rule table data
   * @param wcPrev previous full dataframe
   * @param i (1 for wc10, 2 for wc20 )
   */
  def createWcCodes(salesRule: DataFrame, wcPrev:DataFrame, i:Int ):DataFrame= {
    val wc = getCode(salesRule,i)
    val wcFull = MergeUtils.InsertUpdateMerge(wcPrev, wc, SalesRuleVariables.FK_CUSTOMER)
    wcFull
  }

}
