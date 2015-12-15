package com.jabong.dap.export.dcf

import java.sql.Timestamp

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.clickstream.ClickStreamConstant
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by rahul on 20/8/15.
 */
object DcfFeedGenerator extends Logging {

  /**
   * Starting point of dcf feed generation
   * @param params
   */
  def start(params: ParamInfo) {
    logger.info("dcf feed generation process started")
    val hiveContext = Spark.getHiveContext()
    val executeDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val clickstreamTable = OptionUtils.getOptValue(params.input, ClickStreamConstant.MERGE_PAGEVISIT)
    val monthYear = TimeUtils.getMonthAndYear(executeDate, TimeConstants.DATE_FORMAT_FOLDER)
    val month = monthYear.month + 1
    val date = monthYear.day
    val year = monthYear.year

    val cmr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, executeDate)

    val hiveQuery = "SELECT userid, productsku,pagets,sessionid FROM " + clickstreamTable +
      " where pagetype in ('CPD','QPD','DPD') and pagets is not null and sessionid is not null and sessionid != '(null)' and " +
      "date1 = " + date + " and month1 = " + month + " and year1=" + year

    logger.info("Running hive query :- " + hiveContext)

    val pageVisitData = hiveContext.sql(hiveQuery)

    val joinedData = convertFeedFormat(pageVisitData, cmr).cache()
    val changedDateFormat = TimeUtils.changeDateFormat(executeDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)
    val writePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.DCF_FEED, DataSets.CLICKSTREAM_MERGED_FEED, DataSets.FULL, executeDate)

    DataWriter.writeParquet(joinedData, writePath, saveMode)

    DataWriter.writeCsv(joinedData, DataSets.DCF_FEED, DataSets.CLICKSTREAM_MERGED_FEED, DataSets.FULL, executeDate, DataSets.DCF_FEED_FILENAME + changedDateFormat + "_1", DataSets.ERROR_SAVEMODE, "false", ",", 1)

    logger.info("dcf feed generation process ended")
  }

  /**
   * Converts feed into format required by DCF
   * @param pageVisitData
   * @param deviceMapping
   * @return
   */
  def convertFeedFormat(pageVisitData: DataFrame, deviceMapping: DataFrame): DataFrame = {

    logger.info("joining started :- pagevisit with deviceMapping to get customerId")

    val joinedData = pageVisitData.join(deviceMapping, pageVisitData("userid") === deviceMapping("email"), SQL.LEFT_OUTER)
      .select(
        deviceMapping("id_customer") as "uid",
        pageVisitData("productsku") as "sku",
        changeDateFormatValue(pageVisitData("pagets"), lit("yyyy-MM-dd HH:mm:ss.SSS"), lit("yyyy-MM-dd'T'HH:mm:ss'Z'")) as "date_created",
        pageVisitData("sessionid") as "sessionId"
      )
    logger.info("joining ended :- pagevisit with deviceMapping to get customerId")

    return joinedData
  }

  // Udf to change date format
  val changeDateFormatValue = udf((date: Timestamp, initialFormat: String, expectedFormat: String) => TimeUtils.changeDateFormat(date: Timestamp, initialFormat: String, expectedFormat: String))

}
