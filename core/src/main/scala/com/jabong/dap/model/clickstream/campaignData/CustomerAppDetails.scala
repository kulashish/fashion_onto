package com.jabong.dap.model.clickstream.campaignData

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ CustomerVariables, SalesOrderVariables }
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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

  val DOMAIN = "domain"
  val CREATED_AT = "created_at"

  val TEMP_ = "temp_"
  val CUSTOMER_SESSION_ID = "customer_session_id"

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    val incrSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.DAILY_MODE, incrDate)
    val fullSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.FULL_MERGE_MODE, incrDate)

    DataWriter.canWrite(saveMode, incrSavePath) || DataWriter.canWrite(saveMode, fullSavePath)
  }

  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {
    logger.info("readDF called")
    val salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
    val cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)
    val customerSessionIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, CUSTOMER_SESSION, DataSets.DAILY_MODE, incrDate)

    val custAppDetailsPrevFull =
      if (null != paths) {
        val salesOrderFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate) //:TODO need to check path
        getFullOnFirstDay(prevDate, cmrFull, salesOrderFull)
      } else {
        DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.FULL_MERGE_MODE, prevDate)
      }

    val dfMap: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()
    dfMap.put("custAppDetailsPrevFull", custAppDetailsPrevFull)
    dfMap.put("salesOrderIncr", salesOrderIncr)
    dfMap.put("cmrFull", cmrFull)
    dfMap.put("customerSessionIncr", customerSessionIncr)
    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    logger.info("process called")
    val custAppDetailsPrevFull = dfMap.getOrElse("custAppDetailsPrevFull", null)
    val salesOrderIncr = dfMap("salesOrderIncr")
    val cmrFull = dfMap("cmrFull")
    val customerSessionIncr = dfMap("customerSessionIncr")
    //calculating in two parts
    //ID_CUSTOMER FIRST_LOGIN_TIME LAST_LOGIN_TIME SESSION_KEY
    //ID_CUSTOMER: DOMAIN, CREATED_AT, ORDER_COUNT
    val trimmedSO = salesOrderIncr.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.CUSTOMER_SESSION_ID, CREATED_AT, CustomerVariables.DOMAIN)
      .filter(col(SalesOrderVariables.FK_CUSTOMER).isNotNull && col(DOMAIN).isNotNull)
    val custDomain = trimmedSO.select(SalesOrderVariables.FK_CUSTOMER, DOMAIN).filter("domain in ('ios', 'android', 'windows')").distinct

    val custOrderCountCreatedAt = trimmedSO.select(SalesOrderVariables.FK_CUSTOMER, CREATED_AT).groupBy(SalesOrderVariables.FK_CUSTOMER).agg(count(CREATED_AT).cast(IntegerType) as ORDER_COUNT, max(CREATED_AT) as CREATED_AT)
      .withColumnRenamed(SalesOrderVariables.FK_CUSTOMER, TEMP_ + SalesOrderVariables.FK_CUSTOMER)
    val exceptSession = custDomain.join(custOrderCountCreatedAt, custOrderCountCreatedAt(TEMP_ + SalesOrderVariables.FK_CUSTOMER) === custDomain(SalesOrderVariables.FK_CUSTOMER), SQL.LEFT_OUTER)
      .drop(TEMP_ + CREATED_AT).drop(TEMP_ + SalesOrderVariables.FK_CUSTOMER)
      .withColumnRenamed(SalesOrderVariables.FK_CUSTOMER, TEMP_ + SalesOrderVariables.FK_CUSTOMER)
      .withColumnRenamed(CREATED_AT, TEMP_ + CREATED_AT)
    val soWithoutDomain = trimmedSO.drop(DOMAIN).groupBy(SalesOrderVariables.FK_CUSTOMER, CREATED_AT).agg(first(CUSTOMER_SESSION_ID) as CUSTOMER_SESSION_ID)
    val exceptLoginTime = exceptSession.join(soWithoutDomain, col(TEMP_ + SalesOrderVariables.FK_CUSTOMER) === col(SalesOrderVariables.FK_CUSTOMER) && col(CREATED_AT) === col(TEMP_ + CREATED_AT), SQL.LEFT_OUTER)
      .filter(col(CREATED_AT).isNotNull && col(SalesOrderVariables.FK_CUSTOMER).isNotNull && col(CUSTOMER_SESSION_ID).isNotNull)
      .drop(TEMP_ + SalesOrderVariables.FK_CUSTOMER)
      .drop(TEMP_ + CREATED_AT)
      .withColumnRenamed(CUSTOMER_SESSION_ID, SESSION_KEY)
    //Done: FK_CUSTOMER ORDER_COUNT CREATED_AT DOMAIN SESSION
    //NOW: LOGIN INFO from CUST_SESSION

    val trimmedCustomerSession = customerSessionIncr.select(SESSION_KEY, LOGIN_TIME)
      .filter(col(SESSION_KEY).isNotNull && col(LOGIN_TIME).isNotNull)
      .groupBy(SESSION_KEY)
      .agg(min(LOGIN_TIME) as FIRST_LOGIN_TIME, max(LOGIN_TIME) as LAST_LOGIN_TIME)
      .withColumnRenamed(SESSION_KEY, TEMP_ + SESSION_KEY)
    val withLoginTime = exceptLoginTime.join(trimmedCustomerSession, trimmedCustomerSession(TEMP_ + SESSION_KEY) === exceptLoginTime(SESSION_KEY), SQL.LEFT_OUTER)
      .drop(TEMP_ + SESSION_KEY)
    val trimmedCMR = cmrFull.select("UID", CustomerVariables.ID_CUSTOMER).withColumnRenamed("UID", UID).filter(col(UID).isNotNull && col(CustomerVariables.ID_CUSTOMER).isNotNull)
    val incr = withLoginTime.join(trimmedCMR, withLoginTime(SalesOrderVariables.FK_CUSTOMER) === trimmedCMR(CustomerVariables.ID_CUSTOMER), SQL.INNER)
      .filter(col(UID).isNotNull && col(DOMAIN).isNotNull)
      .drop(SalesOrderVariables.FK_CUSTOMER)
      .drop(CustomerVariables.ID_CUSTOMER)
    val master = SchemaUtils.renameCols(custAppDetailsPrevFull, TEMP_)
    logger.info("Updating master")
    val updatedMaster = master.join(incr, master(TEMP_ + UID) === incr(UID) && master(TEMP_ + DOMAIN) === incr(DOMAIN), SQL.FULL_OUTER)
      .select(
        coalesce(master(TEMP_ + UID), incr(UID)) as UID,
        coalesce(master(TEMP_ + DOMAIN), incr(DOMAIN)) as DOMAIN,
        coalesce(incr(CREATED_AT), master(TEMP_ + CREATED_AT)) as CREATED_AT,
        coalesce(master(TEMP_ + FIRST_LOGIN_TIME), incr(FIRST_LOGIN_TIME)) as FIRST_LOGIN_TIME,
        coalesce(master(TEMP_ + LAST_LOGIN_TIME), incr(LAST_LOGIN_TIME)) as LAST_LOGIN_TIME,
        coalesce(incr(SESSION_KEY), master(TEMP_ + SESSION_KEY)) as SESSION_KEY,
        when(incr(ORDER_COUNT).isNotNull && master(TEMP_ + ORDER_COUNT).isNotNull, (incr(ORDER_COUNT) + master(TEMP_ + ORDER_COUNT)).cast(IntegerType))
          .otherwise(when(incr(ORDER_COUNT).isNotNull, incr(ORDER_COUNT).cast(IntegerType)).otherwise(master(TEMP_ + ORDER_COUNT).cast(IntegerType))) as ORDER_COUNT)
      .select(UID, DOMAIN, CREATED_AT, FIRST_LOGIN_TIME, LAST_LOGIN_TIME, SESSION_KEY, ORDER_COUNT)

    val incrDistinctUID = incr.select(UID, DOMAIN).distinct.withColumnRenamed(UID, TEMP_ + UID).withColumnRenamed(DOMAIN, TEMP_ + DOMAIN)
    val incrForExport = incrDistinctUID.join(updatedMaster, incrDistinctUID(TEMP_ + UID) === updatedMaster(UID) && incrDistinctUID(TEMP_ + DOMAIN) === updatedMaster(DOMAIN), SQL.INNER)
      .select(UID, DOMAIN, CREATED_AT, FIRST_LOGIN_TIME, LAST_LOGIN_TIME, SESSION_KEY, ORDER_COUNT)
    val dfWriteMap: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()
    dfWriteMap.put("custAppDetailsFull", updatedMaster)
    dfWriteMap.put("custAppDetailsIncr", incrForExport)
    dfWriteMap
  }

  def write(dfWriteMap: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    logger.info("write called")
    val fullSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(fullSavePath, saveMode)) {
      DataWriter.writeParquet(dfWriteMap("custAppDetailsFull"), fullSavePath, saveMode)
    }

    val custAppDetailsIncr = dfWriteMap("custAppDetailsIncr")
    val incrSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.DAILY_MODE, incrDate)
    if (DataWriter.canWrite(incrSavePath, saveMode)) {
      DataWriter.writeParquet(custAppDetailsIncr, incrSavePath, saveMode)
    }
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(custAppDetailsIncr, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.DAILY_MODE, incrDate, fileDate + "_Customer_App_details", saveMode, "true", ";")
  }

  def getFullOnFirstDay(date: String, cmr: DataFrame, salesOrderFull: DataFrame): DataFrame = {
    def calculateOtherFields(salesOrderFull: DataFrame): DataFrame = {
      logger.info("calculateOtherFields called")
      val custDomainSORenamed = salesOrderFull.select(SalesOrderVariables.FK_CUSTOMER, DOMAIN).distinct
        .withColumnRenamed(SalesOrderVariables.FK_CUSTOMER, TEMP_ + SalesOrderVariables.FK_CUSTOMER)
      val custSalesCount = salesOrderFull.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.CREATED_AT)
        .groupBy(SalesOrderVariables.FK_CUSTOMER).agg(count(CREATED_AT).cast(IntegerType) as ORDER_COUNT, max(CREATED_AT) as CREATED_AT)
      val fullOtherFields = custDomainSORenamed.join(custSalesCount, custDomainSORenamed(TEMP_ + SalesOrderVariables.FK_CUSTOMER) === custSalesCount(SalesOrderVariables.FK_CUSTOMER), SQL.LEFT_OUTER)
        .drop(custDomainSORenamed(TEMP_ + SalesOrderVariables.FK_CUSTOMER))
      fullOtherFields
    }
    logger.info("getFullOnFirstDay called")
    val inputCSVFile = "CUSTOMER_APP_DETAILS_" + TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD) + ".csv"

    //here the first time dump should be kept - we'll take all the session info from previous system as we dont have full CustSession. Other fields we'll calculate
    val dfFromPrevSys = DataReader.getDataFrame4mCsv(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_APP_DETAILS, DataSets.FULL, date, inputCSVFile, "true", "|")
    val correctSchemaDF = dfFromPrevSys.select(col("ID_CUSTOMER").cast(LongType) as CustomerVariables.ID_CUSTOMER,
      col("LOGIN_TIME").cast(TimestampType) as FIRST_LOGIN_TIME,
      col("LAST_LOGIN_TIME").cast(TimestampType) as LAST_LOGIN_TIME,
      col("SESSION_KEY") as SESSION_KEY)

    val custLastLogin = correctSchemaDF.groupBy(CustomerVariables.ID_CUSTOMER).agg(min(FIRST_LOGIN_TIME) as FIRST_LOGIN_TIME, max(LAST_LOGIN_TIME) as LAST_LOGIN_TIME)
    val custLastLogRenamed = SchemaUtils.renameCols(custLastLogin, TEMP_)
    val custSessionLastLog = correctSchemaDF.select(CustomerVariables.ID_CUSTOMER, SESSION_KEY, LAST_LOGIN_TIME).groupBy(CustomerVariables.ID_CUSTOMER, LAST_LOGIN_TIME).agg(first(SESSION_KEY) as SESSION_KEY)
    val custSessionLoginRenamed = custLastLogRenamed.join(custSessionLastLog, custLastLogRenamed(TEMP_ + CustomerVariables.ID_CUSTOMER) === custSessionLastLog(CustomerVariables.ID_CUSTOMER) && custLastLogRenamed(TEMP_ + LAST_LOGIN_TIME) === custSessionLastLog(LAST_LOGIN_TIME), SQL.LEFT_OUTER)
      .drop(custLastLogRenamed(TEMP_ + CustomerVariables.ID_CUSTOMER))
      .drop(custLastLogRenamed(TEMP_ + LAST_LOGIN_TIME))
      .withColumnRenamed(CustomerVariables.ID_CUSTOMER, TEMP_ + CustomerVariables.ID_CUSTOMER)
      .withColumnRenamed(TEMP_ + FIRST_LOGIN_TIME, FIRST_LOGIN_TIME)

    //now it contains ID_CUSTOMER FIRST_LOGIN_TIME LAST_LOGIN_TIME SESSION_KEY
    // getting other fields along with ID_CUSTOMER: DOMAIN, CREATED_AT, ORDER_COUNT

    val otherFields = calculateOtherFields(salesOrderFull)
    val oneTimeFull = custSessionLoginRenamed.join(otherFields, custSessionLoginRenamed(TEMP_ + CustomerVariables.ID_CUSTOMER) === otherFields(CustomerVariables.FK_CUSTOMER), SQL.INNER)
      .drop(custSessionLoginRenamed(TEMP_ + CustomerVariables.ID_CUSTOMER))
      .filter(col(CustomerVariables.FK_CUSTOMER).isNotNull)
      .filter(col(DOMAIN).isNotNull)
      .filter("domain in ('ios', 'android', 'windows')")
    val trimmedCMRRenamed = cmr.select("UID", CustomerVariables.ID_CUSTOMER)
      .filter(col("UID").isNotNull).filter(col(CustomerVariables.ID_CUSTOMER).isNotNull)
      .withColumnRenamed("UID", UID).withColumnRenamed(CustomerVariables.ID_CUSTOMER, "temp_" + CustomerVariables.ID_CUSTOMER).distinct
    val fullWithUID = oneTimeFull.join(trimmedCMRRenamed, oneTimeFull(CustomerVariables.FK_CUSTOMER) === trimmedCMRRenamed(TEMP_ + CustomerVariables.ID_CUSTOMER), SQL.INNER)
      .select(UID, DOMAIN, CREATED_AT, FIRST_LOGIN_TIME, LAST_LOGIN_TIME, SESSION_KEY, ORDER_COUNT)
      .filter(col(UID).isNotNull)
      .filter(col(DOMAIN).isNotNull)
    fullWithUID.printSchema()
    fullWithUID
  }
}