package com.jabong.dap.model.clickstream.campaignData

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{CustomerVariables, SalesOrderVariables}
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, LongType, IntegerType}

import scala.collection.mutable.HashMap

/**
 * Created by kapil on 13/10/15.
 */
object CustomerAppDetails extends DataFeedsModel with Logging {
  val UID = "uid"
  val SESSION_KEY = "session_key"
  val LOGIN_TIME = "login_time"
  val LAST_LOGIN_TIME = "last_login_time"
  val FIRST_LOGIN_TIME = "first_login_time"
  val ORDER_COUNT = "order_count"
  val CUSTOMER_SESSION = "customer_session"

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    val incrSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.DAILY_MODE, incrDate)
    val fullSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.FULL_MERGE_MODE, incrDate)

    DataWriter.canWrite(saveMode, incrSavePath) || DataWriter.canWrite(saveMode, fullSavePath)
  }

  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {

    val salesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
    val cmr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)
    val customerSession = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, CUSTOMER_SESSION, DataSets.DAILY_MODE, incrDate)

    val masterRecord =
      if (null != paths) getFullOnFirstDay(prevDate, cmr)
      else
        DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.FULL_MERGE_MODE, prevDate)

    val dfMap: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()
    dfMap.put("masterRecord", masterRecord)
    dfMap.put("salesOrder", salesOrder)
    dfMap.put("cmr", cmr)
    dfMap.put("customerSession", customerSession)
    dfMap
  }
  def getFullOnFirstDay(date: String, cmr: DataFrame): DataFrame = {
    val inputCSVFile = "CUSTOMER_APP_DETAILS_" + TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD) + ".csv"
    val df = DataReader.getDataFrame4mCsv(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.FULL, date, inputCSVFile, "true", "|")
    val correctSchemaDF = df.select(col("ID_CUSTOMER").cast(LongType) as CustomerVariables.ID_CUSTOMER,
              col("DOMAIN") as CustomerVariables.DOMAIN,
              col("CREATED_AT").cast(TimestampType) as SalesOrderVariables.CREATED_AT,
              col("LOGIN_TIME").cast(TimestampType) as FIRST_LOGIN_TIME,
              col("LAST_LOGIN_TIME").cast(TimestampType) as LAST_LOGIN_TIME,
              col("SESSION_KEY") as SESSION_KEY,
              col("ORDER_COUNT").cast(IntegerType) as ORDER_COUNT)
    val trimmedCMR = cmr.filter("domain in ('ios', 'android', 'windows')")
      .select("UID", CustomerVariables.ID_CUSTOMER).withColumnRenamed("UID", UID).withColumnRenamed(CustomerVariables.ID_CUSTOMER, "temp_"+CustomerVariables.ID_CUSTOMER)

    val withUID = correctSchemaDF.join(trimmedCMR, correctSchemaDF(CustomerVariables.ID_CUSTOMER) === trimmedCMR("temp_"+CustomerVariables.ID_CUSTOMER), SQL.LEFT_OUTER)
                  .drop(trimmedCMR("temp_"+CustomerVariables.ID_CUSTOMER))
                  .drop(correctSchemaDF(CustomerVariables.ID_CUSTOMER))
    withUID
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val masterRecord = dfMap("masterRecord")
    val salesOrder = dfMap("salesOrder")
    val cmr = dfMap("cmr")
    val customerSession = dfMap("customerSession")

    //trimming
    val trimmedSalesOrder = salesOrder.filter("domain in ('ios', 'android', 'windows')")
      .select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.CUSTOMER_SESSION_ID, SalesOrderVariables.CREATED_AT, CustomerVariables.DOMAIN)
      .groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(
        count(SalesOrderVariables.CREATED_AT).cast(IntegerType) as ORDER_COUNT,
        last(SalesOrderVariables.CUSTOMER_SESSION_ID) as SalesOrderVariables.CUSTOMER_SESSION_ID,
        last(CustomerVariables.DOMAIN) as CustomerVariables.DOMAIN,
        last(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.CREATED_AT)

    val trimmedCMR = cmr.filter("domain in ('ios', 'android', 'windows')")
      .select("UID", CustomerVariables.ID_CUSTOMER).withColumnRenamed("UID", UID)

    val trimmedCustomerSession = customerSession.select(SESSION_KEY, LOGIN_TIME)
      .groupBy(SESSION_KEY)
      .agg(min(LOGIN_TIME) as FIRST_LOGIN_TIME, max(LOGIN_TIME) as LAST_LOGIN_TIME)

    val soWithUID = trimmedSalesOrder.join(trimmedCMR, trimmedSalesOrder(SalesOrderVariables.FK_CUSTOMER) === trimmedCMR(CustomerVariables.ID_CUSTOMER), SQL.LEFT_OUTER)
      .drop(trimmedCMR(CustomerVariables.ID_CUSTOMER))
      .drop(trimmedSalesOrder(SalesOrderVariables.FK_CUSTOMER))

    val soWithUIDLoginTime = soWithUID.join(trimmedCustomerSession, soWithUID(SalesOrderVariables.CUSTOMER_SESSION_ID) === trimmedCustomerSession(SESSION_KEY), SQL.LEFT_OUTER)
      .drop(soWithUID(SalesOrderVariables.CUSTOMER_SESSION_ID))
    val INCR_ = "incr_"
    val MASTER_ = "master_"

    val masterRecordRenamed = SchemaUtils.renameCols(masterRecord, MASTER_)

    val incrDF = soWithUIDLoginTime.join(masterRecordRenamed, soWithUIDLoginTime(UID) === masterRecordRenamed(MASTER_ + UID), SQL.LEFT_OUTER)
      .select(
        coalesce(masterRecordRenamed(MASTER_ + UID), soWithUIDLoginTime(UID)) as UID,

        coalesce(soWithUIDLoginTime(CustomerVariables.DOMAIN), masterRecordRenamed(MASTER_ + CustomerVariables.DOMAIN)) as CustomerVariables.DOMAIN,
        coalesce(soWithUIDLoginTime(CustomerVariables.CREATED_AT), masterRecordRenamed(MASTER_ + CustomerVariables.CREATED_AT)) as CustomerVariables.CREATED_AT,
        coalesce(masterRecordRenamed(MASTER_ + FIRST_LOGIN_TIME), soWithUIDLoginTime(FIRST_LOGIN_TIME)) as FIRST_LOGIN_TIME,
        coalesce(soWithUIDLoginTime(LAST_LOGIN_TIME), masterRecordRenamed(MASTER_ + LAST_LOGIN_TIME)) as LAST_LOGIN_TIME,
        coalesce(soWithUIDLoginTime(SESSION_KEY), masterRecordRenamed(MASTER_ + SESSION_KEY)) as SESSION_KEY,
        when(masterRecordRenamed(MASTER_ + ORDER_COUNT).isNull, soWithUIDLoginTime(ORDER_COUNT).cast(IntegerType))
          .otherwise((masterRecordRenamed(MASTER_ + ORDER_COUNT) + soWithUIDLoginTime(ORDER_COUNT)).cast(IntegerType))
          as ORDER_COUNT)

    val incrRenamed = SchemaUtils.renameCols(incrDF, INCR_)

    val updatedMaster = masterRecord.join(incrRenamed, incrRenamed(INCR_ + UID) === masterRecord(UID), SQL.FULL_OUTER)
      .select(
        coalesce(incrRenamed(INCR_ + UID), masterRecord(UID)) as UID,
        coalesce(incrRenamed(INCR_ + CustomerVariables.DOMAIN), masterRecord(CustomerVariables.DOMAIN)) as CustomerVariables.DOMAIN,
        coalesce(incrRenamed(INCR_ + CustomerVariables.CREATED_AT), masterRecord(CustomerVariables.CREATED_AT)) as CustomerVariables.CREATED_AT,
        coalesce(masterRecord(FIRST_LOGIN_TIME), incrRenamed(INCR_ + FIRST_LOGIN_TIME)) as FIRST_LOGIN_TIME,
        coalesce(incrRenamed(INCR_ + LAST_LOGIN_TIME), masterRecord(LAST_LOGIN_TIME)) as LAST_LOGIN_TIME,
        coalesce(incrRenamed(INCR_ + SESSION_KEY), masterRecord(SESSION_KEY)) as SESSION_KEY,
        when(incrRenamed(INCR_ + ORDER_COUNT).isNotNull && masterRecord(ORDER_COUNT).isNotNull, (masterRecord(ORDER_COUNT) + incrRenamed(INCR_ + ORDER_COUNT)).cast(IntegerType))
          .otherwise(when(incrRenamed(INCR_ + ORDER_COUNT).isNotNull, incrRenamed(INCR_ + ORDER_COUNT).cast(IntegerType)).otherwise(masterRecord(ORDER_COUNT).cast(IntegerType))) as ORDER_COUNT)
    val dfWriteMap: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()
    dfWriteMap.put("updatedMaster", updatedMaster)
    dfWriteMap.put("incrDF", incrDF)
    dfWriteMap
  }
  def write(dfWriteMap: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val full = dfWriteMap("updatedMaster")
    val incr = dfWriteMap("incrDF")
    val incrSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.DAILY_MODE, incrDate)
    val fullSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.FULL_MERGE_MODE, incrDate)
    val csvFileName = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD) + "_Customer_App_details"
    if (DataWriter.canWrite(incrSavePath, saveMode)) {
      DataWriter.writeParquet(incr, incrSavePath, saveMode)
    }
    if (DataWriter.canWrite(fullSavePath, saveMode)) {
      DataWriter.writeParquet(full, fullSavePath, saveMode)
    }
    DataWriter.writeCsv(incr, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.DAILY_MODE, incrDate, csvFileName, saveMode, "true", ";")
  }
}