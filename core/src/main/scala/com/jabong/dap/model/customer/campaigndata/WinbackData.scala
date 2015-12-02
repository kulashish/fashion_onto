package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CrmTicketVariables, CustomerVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

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
    dateStr = incrDate

    val dateDiffFormat = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)
    val crmTicketMasterIncr = DataReader.getDataFrame4mOrc(ConfigConstants.INPUT_PATH, DataSets.CRM, DataSets.CRM_TicketMaster, DataSets.FULL_FETCH_MODE, dateDiffFormat)
    println(ConfigConstants.INPUT_PATH + "/" + DataSets.CRM + "/" + DataSets.CRM_TicketMaster)
    val crmTicketDetailsIncr = DataReader.getDataFrame4mOrc(ConfigConstants.INPUT_PATH, DataSets.CRM, DataSets.CRM_TicketDetails, DataSets.DAILY_MODE, incrDate)
    val crmTicketStatLogIncr = DataReader.getDataFrame4mOrc(ConfigConstants.INPUT_PATH, DataSets.CRM, DataSets.CRM_TicketStatusLog, DataSets.DAILY_MODE, incrDate)

    val fullSalesOrder = CampaignInput.loadFullOrderData(incrDate)
    val days_45Order = CampaignInput.loadLastNdaysOrderData(45, fullSalesOrder, prevDate)

    val cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)

    val dfMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]

    dfMap.put("crmTicketMasterIncr", crmTicketMasterIncr)
    crmTicketMasterIncr.printSchema()
    println(crmTicketMasterIncr.count)
    println(crmTicketMasterIncr.distinct.count)

    dfMap.put("crmTicketDetailsIncr", crmTicketDetailsIncr)
    crmTicketDetailsIncr.printSchema()
    println(crmTicketDetailsIncr.count)
    println(crmTicketDetailsIncr.distinct.count)

    dfMap.put("crmTicketStatLogIncr", crmTicketStatLogIncr)
    dfMap.put("cmrFull", cmrFull)
    dfMap.put("salesOrder", days_45Order)

    dfMap
  }

  override def write(dfWrite: mutable.HashMap[String, DataFrame], saveMode: String, incrDate: String): Unit = {
    val result = dfWrite("result")
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

    DataWriter.writeCsv(result, DataSets.VARIABLES, DataSets.WINBACK_CUSTOMER, DataSets.DAILY_MODE, incrDate, fileDate + "winback_customer_data", saveMode, "true", ";")
  }

  override def process(dfMap: mutable.HashMap[String, DataFrame]): mutable.HashMap[String, DataFrame] = {

    val crmTicketMasterIncr = dfMap("crmTicketMasterIncr")
    val crmTicketDetailsIncr = dfMap("crmTicketDetailsIncr")
    val crmTicketStatLogIncr = dfMap("crmTicketStatLogIncr")
    val cmrFull = dfMap("cmrFull")
    val salesOrder = dfMap("salesOrder")

    val yesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, dateStr)

    //TODO: use constant for the status with  meaningful name
    //TODO:  use  better implementation instead of join for
    /*
    select id from cmr where not exists
    (select fk from salesOrder where fk = id)
     */

    val result =
      crmTicketDetailsIncr.
        join(crmTicketStatLogIncr, crmTicketDetailsIncr(CrmTicketVariables.TICKET_ID) === crmTicketStatLogIncr(CrmTicketVariables.TICKET_ID), SQL.INNER).
        join(crmTicketMasterIncr, crmTicketMasterIncr(CrmTicketVariables.ISSUE_ID) === crmTicketDetailsIncr(CrmTicketVariables.ISSUE_ID), SQL.INNER).
        join(cmrFull, crmTicketDetailsIncr(CrmTicketVariables.CUSTOMER_NO) === cmrFull(CustomerVariables.ID_CUSTOMER), SQL.INNER).
        join(salesOrder, cmrFull(CustomerVariables.ID_CUSTOMER) === salesOrder(SalesOrderVariables.FK_CUSTOMER)).
      withColumnRenamed(CrmTicketVariables.IN_DT, CrmTicketVariables.DG_END_DATE).
        where(
          crmTicketStatLogIncr(CrmTicketVariables.ADD_DATE).geq(yesterday).
            and(crmTicketStatLogIncr(CrmTicketVariables.EXIT_TICKET_STATUS).cast(IntegerType) equalTo (21)).
            and(crmTicketDetailsIncr(CrmTicketVariables.DG_END_DATE).gt(dateStr)).
            and (crmTicketMasterIncr(CrmTicketVariables.DG_END_DATE).gt(dateStr)).
            and(crmTicketStatLogIncr(CrmTicketVariables.DG_END_DATE).gt(dateStr)).
            and(crmTicketDetailsIncr(CrmTicketVariables.ORDER_NO).notEqual(0)).
            and(cmrFull(CustomerVariables.ID_CUSTOMER).notEqual(0)).
            and(salesOrder(SalesOrderVariables.ID_SALES_ORDER).isNull)
        ).select(
            cmrFull(ContactListMobileVars.UID) as ContactListMobileVars.UID,
            crmTicketDetailsIncr(CrmTicketVariables.ORDER_NO) as CrmTicketVariables.ORDER_NO,
            crmTicketStatLogIncr(CrmTicketVariables.ADD_DATE) as CrmTicketVariables.ADD_DATE,
            crmTicketDetailsIncr(CrmTicketVariables.DG_END_DATE) as CrmTicketVariables.TICKET_CLOSE_DATE,
            crmTicketMasterIncr(CrmTicketVariables.ISSUE_ID) as CrmTicketVariables.ISSUE_ID,
            crmTicketMasterIncr(CrmTicketVariables.ISSUE_DESCRIPTION) as CrmTicketVariables.ISSUE_DESCRIPTION
          )

    val incrMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]

    incrMap.put("result", result)

    incrMap

  }
}
