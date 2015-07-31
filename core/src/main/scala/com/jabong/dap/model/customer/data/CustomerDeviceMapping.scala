package com.jabong.dap.model.customer.data

import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.common.constants.variables.{ CustomerVariables, PageVisitVariables }
import com.jabong.dap.data.acq.common.VarInfo
import com.jabong.dap.data.read.{ DataNotFound, DataReader, ValidFormatNotFound }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType


/**
 * Created by mubarak on 15/7/15.
 */
object CustomerDeviceMapping extends Logging {

  /**
   *
   * @param clickStreamInc incremental click_stream data
   * @param dcf dcf customer->device_mapping data
   * @param customer customer incremental data
   * @return master customer device mapping with the last used device by the customer
   */
  def getLatestDevice(clickStreamInc: DataFrame, dcf: DataFrame, customer: DataFrame): DataFrame = {
    //val filData = clickStreamInc.filter(!clickStreamInc(PageVisitVariables.USER_ID).startsWith(CustomerVariables.APP_FILTER))
    val clickStream = clickStreamInc.orderBy(PageVisitVariables.PAGE_TIMESTAMP).groupBy(PageVisitVariables.USER_ID).agg(
      first(PageVisitVariables.BROWSER_ID) as PageVisitVariables.BROWSER_ID,
      first(PageVisitVariables.DOMAIN) as PageVisitVariables.DOMAIN)

    // outerjoin with customer table one day increment on userid = email
    // id_customer, email, browser_id, domain
    val broCust = Spark.getContext().broadcast(customer).value
    val joinedDf = clickStream.join(broCust, broCust(CustomerVariables.EMAIL) === clickStream(PageVisitVariables.USER_ID), "outer")
      .select(coalesce(broCust(CustomerVariables.EMAIL), clickStream(PageVisitVariables.USER_ID)) as CustomerVariables.EMAIL,
        broCust(CustomerVariables.ID_CUSTOMER),
        clickStream(PageVisitVariables.BROWSER_ID),
        clickStream(PageVisitVariables.DOMAIN)
      )
    val joined = joinedDf.join(dcf, dcf(CustomerVariables.EMAIL) === joinedDf(CustomerVariables.EMAIL), "outer").select(
      coalesce(dcf(CustomerVariables.EMAIL), joinedDf(CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,
      dcf(CustomerVariables.RESPONSYS_ID),
      dcf(CustomerVariables.ID_CUSTOMER),
      coalesce(dcf(PageVisitVariables.BROWSER_ID), joinedDf(PageVisitVariables.BROWSER_ID)) as PageVisitVariables.BROWSER_ID,
      coalesce(dcf(PageVisitVariables.DOMAIN), joinedDf(PageVisitVariables.DOMAIN)) as PageVisitVariables.DOMAIN)
    joined
  }

  /**
   *
   * @param vars
   */
  def start(vars: VarInfo) = {
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT))
    val path = OptionUtils.getOptValue(vars.path)
    val saveMode = vars.saveMode
    processData(prevDate, path, incrDate, saveMode)
  }

  /**
   *
   * @param prevDate
   * @param path
   * @param curDate
   */
  def processData(prevDate: String, path: String, curDate: String, saveMode: String) {
    val df1 = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.CLICKSTREAM, DataSets.USER_DEVICE_MAP_APP, DataSets.DAILY_MODE, curDate)
    var df2: DataFrame = null
    if (null != path) {
      df2 = getDataFrameCsv4mDCF(path)
    } else {
      df2 = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, prevDate)
    }
    val df3 = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.DAILY_MODE, curDate)
    val res = getLatestDevice(df1, df2, df3)
    val savePath = DataWriter.getWritePath(DataSets.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, curDate)
    if (DataWriter.canWrite(saveMode, savePath))
      DataWriter.writeParquet(res, savePath, saveMode)
  }

  /**
   *
   * @param path for the dcf csv file
   * @return DataFrame
   */
  def getDataFrameCsv4mDCF(path: String): DataFrame = {
    require(path != null, "Path is null")

    try {
      val df = Spark.getSqlContext().read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").load(path)
        .withColumnRenamed("RESPONSYS_ID", CustomerVariables.RESPONSYS_ID)
        .withColumnRenamed("CUSTOMER_ID", CustomerVariables.ID_CUSTOMER)
        .withColumnRenamed("EMAIL", CustomerVariables.EMAIL)
        .withColumnRenamed("BID", PageVisitVariables.BROWSER_ID)
        .withColumnRenamed("APPTYPE", PageVisitVariables.DOMAIN)
      df
    } catch {
      case e: DataNotFound =>
        logger.error("Data not found for the given path ")
        throw new DataNotFound
      case e: ValidFormatNotFound =>
        logger.error("Format could not be resolved for the given files in directory")
        throw new ValidFormatNotFound
    }
  }

}
