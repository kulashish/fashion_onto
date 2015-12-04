package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{CrmTicketVariables, SalesOrderVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.annotation.elidable
import scala.annotation.elidable._
import scala.collection.mutable

/**
 * Created by samathashetty on 20/11/15.
 */
object WinbackData extends DataFeedsModel {
  var dateStr: String = null
  override def canProcess(incrDate: String, saveMode: String): Boolean = {
    val incrSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.WINBACK_CUSTOMER, DataSets.DAILY_MODE, incrDate)

    DataWriter.canWrite(saveMode, incrSavePath)
  }

  override def readDF(incrDate: String, prevDate: String, paths: String): mutable.HashMap[String, DataFrame] = {

    val dateDiffFormat = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)
    dateStr = dateDiffFormat

    val crmTicketMasterIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.CRM, DataSets.CRM_TicketMaster, DataSets.FULL_FETCH_MODE, dateDiffFormat)
    println(ConfigConstants.INPUT_PATH + "/" + DataSets.CRM + "/" + DataSets.CRM_TicketMaster)
    val crmTicketDetailsIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.CRM, DataSets.CRM_TicketDetails, DataSets.FULL_FETCH_MODE, dateDiffFormat)
    val crmTicketStatLogIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.CRM, DataSets.CRM_TicketStatusLog, DataSets.DAILY_MODE, incrDate)

    val dfMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]

    dfMap.put("crmTicketMasterIncr", crmTicketMasterIncr)

    dfMap.put("crmTicketDetailsIncr", crmTicketDetailsIncr)

    dfMap.put("crmTicketStatLogIncr", crmTicketStatLogIncr)

    println("Status Log Data")
    println(crmTicketStatLogIncr.select("ticketstatuslogid").count)
    println(crmTicketStatLogIncr.select("ticketstatuslogid").distinct.count)
    crmTicketStatLogIncr.printSchema()

    dfMap
  }

  override def write(dfWrite: mutable.HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val result = dfWrite("result")
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

    DataWriter.writeCsv(result, DataSets.VARIABLES, DataSets.WINBACK_CUSTOMER, DataSets.DAILY_MODE, incrDate, fileDate + "winback_customer_data", saveMode, "true", ";")
  }

  override def process(dfMap: mutable.HashMap[String, DataFrame]): mutable.HashMap[String, DataFrame] = {

    val crmTicketMasterIncr = dfMap("crmTicketMasterIncr").select(
      col(CrmTicketVariables.ISSUE_ID),
      col(CrmTicketVariables.ADD_DATE) as CrmTicketVariables.DG_END_DATE,
      col(CrmTicketVariables.ISSUE_DESCRIPTION)
    )

    println("Master Data")
    println(crmTicketMasterIncr.select(CrmTicketVariables.ISSUE_ID).count)
    println(crmTicketMasterIncr.select(CrmTicketVariables.ISSUE_ID).count)
    crmTicketMasterIncr.printSchema()


    val crmTicketDetailsIncr = dfMap("crmTicketDetailsIncr").select(
      col(CrmTicketVariables.ISSUE_ID),
      col(CrmTicketVariables.TICKET_ID),
      col(CrmTicketVariables.IN_DT) as CrmTicketVariables.DG_END_DATE,
      col(CrmTicketVariables.CUSTOMER_NO),
      col(CrmTicketVariables.ORDER_NO),
      col(SalesOrderVariables.ID_SALES_ORDER)
    ).where(col(CrmTicketVariables.CUSTOMER_NO).!==(0).and(col(SalesOrderVariables.ID_SALES_ORDER).isNull))

    println("Details Data")
    println(crmTicketDetailsIncr.select(CrmTicketVariables.TICKET_ID).count)
    println(crmTicketDetailsIncr.select(CrmTicketVariables.TICKET_ID).distinct.count)
    crmTicketDetailsIncr.printSchema()

    val crmTicketStatLogIncr = dfMap("crmTicketStatLogIncr").select(
      col(CrmTicketVariables.TICKET_ID),
      col(CrmTicketVariables.ADD_DATE),
      col(CrmTicketVariables.EXIT_TICKET_STATUS),
      col(CrmTicketVariables.IN_DT) as CrmTicketVariables.DG_END_DATE
    )


    val yesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT, dateStr)

    //TODO: use constant for the status with  meaningful name

    val result =
      crmTicketDetailsIncr.
        join(crmTicketStatLogIncr, crmTicketDetailsIncr(CrmTicketVariables.TICKET_ID).equalTo(crmTicketStatLogIncr(CrmTicketVariables.TICKET_ID)), SQL.INNER).
        join(crmTicketMasterIncr, crmTicketMasterIncr(CrmTicketVariables.ISSUE_ID).===(crmTicketDetailsIncr(CrmTicketVariables.ISSUE_ID)), SQL.INNER).
//        where(
//          //uniqueTicketStatLog(CrmTicketVariables.ADD_DATE).geq(yesterday).
//            (uniqueTicketStatLog(CrmTicketVariables.EXIT_TICKET_STATUS).cast(IntegerType) equalTo (21)).
//            //and(crmTicketDetailsIncr(CrmTicketVariables.DG_END_DATE).gt(dateStr)).
//            //and (crmTicketMasterIncr(CrmTicketVariables.DG_END_DATE).gt(dateStr)).
//            //and(uniqueTicketStatLog(CrmTicketVariables.DG_END_DATE).gt(dateStr)).
//            and(crmTicketDetailsIncr(CrmTicketVariables.ORDER_NO).notEqual(0)).
//            and(crmTicketDetailsIncr(CrmTicketVariables.CUSTOMER_NO).notEqual(0))
//        ).
 select(
            crmTicketDetailsIncr(CrmTicketVariables.ORDER_NO) as CrmTicketVariables.ORDER_NO,
            //uniqueTicketStatLog(CrmTicketVariables.ADD_DATE) as CrmTicketVariables.ADD_DATE,
            //crmTicketDetailsIncr(CrmTicketVariables.DG_END_DATE) as CrmTicketVariables.TICKET_CLOSE_DATE,
            crmTicketMasterIncr(CrmTicketVariables.ISSUE_ID) as CrmTicketVariables.ISSUE_ID,
            crmTicketMasterIncr(CrmTicketVariables.ISSUE_DESCRIPTION) as CrmTicketVariables.ISSUE_DESCRIPTION,
            crmTicketDetailsIncr(CrmTicketVariables.CUSTOMER_NO)           )

    println("Result Log Data")
    println(result.count())
    println(result.distinct.count())
    result.printSchema()

    val incrMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]

    incrMap.put("result", result)

    incrMap

  }

}
