package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.{SalesOrderItemVariables, SalesOrderVariables}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.model.order.variables.SalesOrderItem
import grizzled.slf4j.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


/**
 * Created by samathashetty on 2/11/15.
 */
class HottestXData extends CustomerSelector with Logging{
  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame =  {
    val days_45_filter = inData.filter(inData(SalesOrderVariables.GW_AMOUNT).<=(1000))
    val days_60_filter = inData.filter(inData(SalesOrderVariables.GW_AMOUNT).>(1000))

    val joinedDf = days_45_filter.join(days_60_filter, SalesOrderVariables.FK_CUSTOMER)

    //val selDf = joinedDf.orderBy(SalesOrderVariables.UPDATED_AT).first()

    (joinedDf)

  }


  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
