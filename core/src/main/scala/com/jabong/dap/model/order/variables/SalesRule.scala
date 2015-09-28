package com.jabong.dap.model.order.variables

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.SalesRuleVariables
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
   * @param c int to filter the welcome code  (1 for wc10, 2 for wc20 )
   * @return DataFrame with the welcome codes
   */
  def getCode(salesRule: DataFrame, c: String): DataFrame = {
    println("get Code")
    salesRule.show(9)
    val filData = salesRule.filter(salesRule(SalesRuleVariables.CODE).startsWith("WC" + c + "0"))
    println("After filtering")
    filData.show(9)
    val wcCode = filData.select(SalesRuleVariables.FK_CUSTOMER, SalesRuleVariables.UPDATED_AT, SalesRuleVariables.CODE, SalesRuleVariables.CREATED_AT, SalesRuleVariables.TO_DATE)
    println("After select from filData")
    wcCode.show(9)
    wcCode
  }

  /**
   *
   * @param salesRule sales_rule table data
   * @param wcPrev previous full dataframe
   *
   */
  def createWcCodes(salesRule: DataFrame, wcPrev: DataFrame): DataFrame = {
    val wc1 = getCode(salesRule, "3")
    println("After getting wc1")
    wc1.printSchema()
    wc1.show(9)
    val wc2 = getCode(salesRule, "5")
    println("After getting wc2")
    wc2.printSchema()
    wc2.show(9)
    var wcfull: DataFrame = null
    val wcIncr = wc1.join(wc2, wc1(SalesRuleVariables.FK_CUSTOMER) === wc2(SalesRuleVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(wc1(SalesRuleVariables.FK_CUSTOMER), wc2(SalesRuleVariables.FK_CUSTOMER)) as SalesRuleVariables.FK_CUSTOMER,
        wc1(SalesRuleVariables.CODE) as SalesRuleVariables.CODE1,
        wc1(SalesRuleVariables.CREATED_AT) as SalesRuleVariables.CODE1_CREATION_DATE,
        wc1(SalesRuleVariables.TO_DATE) as SalesRuleVariables.CODE1_VALID_DATE,
        wc2(SalesRuleVariables.CODE) as SalesRuleVariables.CODE2,
        wc2(SalesRuleVariables.CREATED_AT) as SalesRuleVariables.CODE2_CREATION_DATE,
        wc2(SalesRuleVariables.TO_DATE) as SalesRuleVariables.CODE2_VALID_DATE
      )
    println("After joining wc1 and wc2")
    wcIncr.printSchema()
    wcIncr.show(9)
    if (null == wcPrev) {
      wcfull = wcIncr
      println("Inside first time loop")
    } else {
      wcfull = wcPrev.join(wcIncr, wcPrev(SalesRuleVariables.FK_CUSTOMER) === wcIncr(SalesRuleVariables.FK_CUSTOMER), SQL.FULL_OUTER)
        .select(
          coalesce(wcIncr(SalesRuleVariables.FK_CUSTOMER), wcPrev(SalesRuleVariables.FK_CUSTOMER)) as SalesRuleVariables.FK_CUSTOMER,
          coalesce(wcIncr(SalesRuleVariables.CODE1), wcPrev(SalesRuleVariables.CODE1)) as SalesRuleVariables.CODE1,
          coalesce(wcIncr(SalesRuleVariables.CODE1_CREATION_DATE), wcPrev(SalesRuleVariables.CODE1_CREATION_DATE)) as SalesRuleVariables.CODE1_CREATION_DATE,
          coalesce(wcIncr(SalesRuleVariables.CODE1_VALID_DATE), wcPrev(SalesRuleVariables.CODE1_VALID_DATE)) as SalesRuleVariables.CODE1_VALID_DATE,
          coalesce(wcIncr(SalesRuleVariables.CODE2), wcPrev(SalesRuleVariables.CODE2)) as SalesRuleVariables.CODE2,
          coalesce(wcIncr(SalesRuleVariables.CODE2_CREATION_DATE), wcPrev(SalesRuleVariables.CODE2_CREATION_DATE)) as SalesRuleVariables.CODE2_CREATION_DATE,
          coalesce(wcIncr(SalesRuleVariables.CODE2_VALID_DATE), wcPrev(SalesRuleVariables.CODE2_VALID_DATE)) as SalesRuleVariables.CODE2_VALID_DATE
        )
    }
    println("returning wcFull from salesRule")
    wcfull.printSchema()
    wcfull.show(9)
    wcfull
  }

}
