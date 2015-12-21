package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, CrmTicketVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StringType, IntegerType }

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

    val crmTicketMasterFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.CRM, DataSets.CRM_TicketMaster, DataSets.FULL_FETCH_MODE, dateDiffFormat)
    println(ConfigConstants.INPUT_PATH + "/" + DataSets.CRM + "/" + DataSets.CRM_TicketMaster)
    val crmTicketDetailsFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.CRM, DataSets.CRM_TicketDetails, DataSets.FULL_MERGE_MODE, dateDiffFormat)
    val crmTicketStatLogIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.CRM, DataSets.CRM_TicketStatusLog, DataSets.DAILY_MODE, incrDate)

    val dfCmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE,
      incrDate).filter(col(CustomerVariables.EMAIL).isNotNull).filter(col(CustomerVariables.ID_CUSTOMER) isNotNull).filter(col(ContactListMobileVars.UID) isNotNull).select(
        col(CustomerVariables.ID_CUSTOMER),
        col(ContactListMobileVars.UID)
      ).dropDuplicates()

    val dfMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]

    dfMap.put("crmTicketMasterFull", crmTicketMasterFull)

    dfMap.put("crmTicketDetailsFull", crmTicketDetailsFull)

    dfMap.put("crmTicketStatLogIncr", crmTicketStatLogIncr)
    dfMap.put("cmrFull", dfCmrFull)

    println("Status Log Data")
    println(crmTicketStatLogIncr.select("ticketstatuslogid").count)
    println(crmTicketStatLogIncr.select("ticketstatuslogid").distinct.count)
    crmTicketStatLogIncr.printSchema()

    dfMap
  }

  override def write(dfWrite: mutable.HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val result = dfWrite("result")
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    val savePathIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.WINBACK_CUSTOMER, DataSets.DAILY_MODE, incrDate)
    println("CRMResult in write")
    result.printSchema()
    DataWriter.writeParquet(result, savePathIncr, saveMode)

    val savedDf = DataReader.getDataFrame(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.WINBACK_CUSTOMER, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeCsv(savedDf, DataSets.VARIABLES, DataSets.WINBACK_CUSTOMER, DataSets.DAILY_MODE, incrDate, fileDate + "_winback_cusstomer_data", saveMode, "true", ";", 1)
  }

  override def process(dfMap: mutable.HashMap[String, DataFrame]): mutable.HashMap[String, DataFrame] = {

    val crmTicketMasterFull = dfMap("crmTicketMasterFull").select(
      col(CrmTicketVariables.ISSUE_ID),
      col(CrmTicketVariables.ADD_DATE) as CrmTicketVariables.DG_END_DATE,
      col(CrmTicketVariables.ISSUE_DESCRIPTION)
    )

    println("Master Data")
    println(crmTicketMasterFull.select(CrmTicketVariables.ISSUE_ID).count)
    println(crmTicketMasterFull.select(CrmTicketVariables.ISSUE_ID).count)
    crmTicketMasterFull.printSchema()

    val crmTicketDetailsFull = dfMap("crmTicketDetailsFull").select(
      col(CrmTicketVariables.ISSUE_ID),
      col(CrmTicketVariables.TICKET_ID),
      col(CrmTicketVariables.UP_DT) as CrmTicketVariables.TICKET_CLOSE_DATE,
      col(CrmTicketVariables.CUSTOMER_NO),
      col(CrmTicketVariables.ORDER_NO),
      col(SalesOrderVariables.ID_SALES_ORDER)
    //    ).where(col(CrmTicketVariables.CUSTOMER_NO).!==(0).and(col(SalesOrderVariables.ID_SALES_ORDER).isNotNull)
    )

    println("Details Data")
    println(crmTicketDetailsFull.select(CrmTicketVariables.TICKET_ID).count)
    println(crmTicketDetailsFull.select(CrmTicketVariables.TICKET_ID).distinct.count)
    crmTicketDetailsFull.printSchema()

    val crmTicketStatLogIncr = dfMap("crmTicketStatLogIncr").select(
      col(CrmTicketVariables.TICKET_ID),
      col(CrmTicketVariables.ADD_DATE),
      col(CrmTicketVariables.EXIT_TICKET_STATUS),
      col(CrmTicketVariables.IN_DT) as CrmTicketVariables.DG_END_DATE
    )

    val cmrFull = dfMap("cmrFull")

    val yesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT, dateStr)

    val crmJoin =
      crmTicketDetailsFull.
        join(crmTicketStatLogIncr, crmTicketDetailsFull(CrmTicketVariables.TICKET_ID) === (crmTicketStatLogIncr(CrmTicketVariables.TICKET_ID)), SQL.INNER).
        join(crmTicketMasterFull, crmTicketMasterFull(CrmTicketVariables.ISSUE_ID) === (crmTicketDetailsFull(CrmTicketVariables.ISSUE_ID)), SQL.INNER).
        where(
          //uniqueTicketStatLog(CrmTicketVariables.ADD_DATE).geq(yesterday).
          (crmTicketStatLogIncr(CrmTicketVariables.EXIT_TICKET_STATUS).cast(IntegerType) equalTo (21)).
            //and(crmTicketDetailsFull(CrmTicketVariables.DG_END_DATE).gt(dateStr)).
            //and (crmTicketMasterFull(CrmTicketVariables.DG_END_DATE).gt(dateStr)).
            //and(uniqueTicketStatLog(CrmTicketVariables.DG_END_DATE).gt(dateStr)).
            and(crmTicketDetailsFull(CrmTicketVariables.ORDER_NO).notEqual(0)).
            and(crmTicketDetailsFull(CrmTicketVariables.CUSTOMER_NO).notEqual(0))
        ).
          select(
            crmTicketDetailsFull(CrmTicketVariables.ORDER_NO) as CrmTicketVariables.ORDER_NO,
            Udf.dateCsvFormatWithArgs(crmTicketDetailsFull(CrmTicketVariables.TICKET_CLOSE_DATE), lit(TimeConstants.DATE_TIME_FORMAT), lit(TimeConstants.DD_MMM_YYYY_HH_MM_SS))
              as CrmTicketVariables.TICKET_CLOSE_DATE,
            crmTicketMasterFull(CrmTicketVariables.ISSUE_ID).cast(StringType) as CrmTicketVariables.ISSUE_ID,
            crmTicketMasterFull(CrmTicketVariables.ISSUE_DESCRIPTION) as CrmTicketVariables.ISSUE_DESCRIPTION,
            crmTicketDetailsFull(CrmTicketVariables.CUSTOMER_NO))

    println("Result Log Data")
    println(crmJoin.count())
    println(crmJoin.distinct.count())
    crmJoin.printSchema()

    println(cmrFull.select(CustomerVariables.ID_CUSTOMER).count())
    println(cmrFull.select(CustomerVariables.ID_CUSTOMER).distinct.count())

    val result = crmJoin.join(cmrFull, crmJoin(CrmTicketVariables.CUSTOMER_NO) === cmrFull(CustomerVariables.ID_CUSTOMER), SQL.INNER).
      select(
        cmrFull(ContactListMobileVars.UID),
        crmJoin(CrmTicketVariables.ORDER_NO) as CrmTicketVariables.ORDER_NO,
        crmJoin(CrmTicketVariables.TICKET_CLOSE_DATE) as CrmTicketVariables.TICKET_CLOSE_DATE,
        crmJoin(CrmTicketVariables.ISSUE_ID) as CrmTicketVariables.ISSUE_ID,
        crmJoin(CrmTicketVariables.ISSUE_DESCRIPTION) as CrmTicketVariables.ISSUE_DESCRIPTION
      )

    println("CRMResult Log Data")
    println(result.count())
    println(result.distinct.count())
    result.printSchema()
    val incrMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]

    incrMap.put("result", result)

    incrMap

  }

}

