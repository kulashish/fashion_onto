package com.jabong.dap.model.customer.data

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ CustomerVariables, PageVisitVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.{ DataNotFound, DataReader, ValidFormatNotFound }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
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
   * @param cmr customer->device_mapping data
   * @param customer customer incremental data
   * @return master customer device mapping with the last used device by the customer
   */
  def getLatestDevice(clickStreamInc: DataFrame, cmr: DataFrame, customer: DataFrame): DataFrame = {
    // val filData = clickStreamInc.filter(!clickStreamInc(PageVisitVariables.USER_ID).startsWith(CustomerVariables.APP_FILTER))
    println("clickStreamInc: ") // + clickStreamInc.count())
    // clickStreamInc.printSchema()
    // clickStreamInc.show(10)

    val clickStream = clickStreamInc.filter(PageVisitVariables.DOMAIN + " IN ('" + DataSets.IOS + "', '" + DataSets.ANDROID + "', '" + DataSets.WINDOWS + "')")
      .orderBy(desc(PageVisitVariables.PAGE_TIMESTAMP))
      .groupBy(PageVisitVariables.USER_ID)
      .agg(
        first(PageVisitVariables.BROWSER_ID) as PageVisitVariables.BROWSER_ID,
        first(PageVisitVariables.DOMAIN) as PageVisitVariables.DOMAIN
      )

    println("clickStream after aggregation and filtering: ") // + clickStream.count())
    // clickStream.printSchema()
    // clickStream.show(10)

    // outerjoin with customer table one day increment on userid = email
    // id_customer, email, browser_id, domain
    val custUnq = customer.select(CustomerVariables.ID_CUSTOMER, CustomerVariables.EMAIL).dropDuplicates()
    val broCust = Spark.getContext().broadcast(custUnq).value
    val joinedDf = clickStream.join(broCust, broCust(CustomerVariables.EMAIL) === clickStream(PageVisitVariables.USER_ID), SQL.FULL_OUTER)
      .select(
        coalesce(broCust(CustomerVariables.EMAIL), clickStream(PageVisitVariables.USER_ID)) as CustomerVariables.EMAIL,
        broCust(CustomerVariables.ID_CUSTOMER),
        clickStream(PageVisitVariables.BROWSER_ID),
        clickStream(PageVisitVariables.DOMAIN)
      )

    println("After outer join with customer table: ") // + joinedDf.count())
    // joinedDf.printSchema()
    // joinedDf.show(10)

    val joined = joinedDf.join(cmr, cmr(CustomerVariables.EMAIL) === joinedDf(CustomerVariables.EMAIL), SQL.FULL_OUTER)
      .select(
        coalesce(cmr(CustomerVariables.EMAIL), joinedDf(CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,
        cmr(CustomerVariables.RESPONSYS_ID),
        coalesce(joinedDf(CustomerVariables.ID_CUSTOMER), cmr(CustomerVariables.ID_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        coalesce(joinedDf(PageVisitVariables.BROWSER_ID), cmr(PageVisitVariables.BROWSER_ID)) as PageVisitVariables.BROWSER_ID,
        coalesce(joinedDf(PageVisitVariables.DOMAIN), cmr(PageVisitVariables.DOMAIN)) as PageVisitVariables.DOMAIN
      )

    println("After outer join with dcf or prev days data for device Mapping: " + joined.count())
    // joined.printSchema()
    // joined.show(10)

    println("Distinct email count for device Mapping: " + joined.select(CustomerVariables.EMAIL).distinct.count())

    val result = joined.na
      .fill(
        Map(
          CustomerVariables.ID_CUSTOMER -> 0,
          PageVisitVariables.BROWSER_ID -> ""
        )
      ).dropDuplicates()
    val resWithout0 = result.filter(col(CustomerVariables.ID_CUSTOMER) > 0)
    println("Total count with id_customer > 0: " + resWithout0.count())
    println("Distinct id_customer count for device Mapping: " + resWithout0.select(CustomerVariables.ID_CUSTOMER).distinct.count())

    return result
  }

  /**
   *
   * @param vars
   */
  def start(vars: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))
    val path = OptionUtils.getOptValue(vars.path)
    val saveMode = vars.saveMode
    val clickIncr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.CLICKSTREAM, DataSets.USER_DEVICE_MAP_APP, DataSets.DAILY_MODE, incrDate)
    processData(prevDate, path, incrDate, saveMode, clickIncr)
    processAdd4pushData(prevDate, incrDate, saveMode, clickIncr)
  }

  /**
   *
   * @param prevDate
   * @param path
   * @param curDate
   */
  def processData(prevDate: String, path: String, curDate: String, saveMode: String, clickIncr: DataFrame) {
    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, curDate)
    if (DataWriter.canWrite(saveMode, savePath)) {
      var cmrFull: DataFrame = null
      if (null != path) {
        cmrFull = getDataFrameCsv4mDCF(path)
      } else {
        cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, prevDate)
      }
      val customerIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.DAILY_MODE, curDate)
      val res = getLatestDevice(clickIncr, cmrFull, customerIncr)

      DataWriter.writeParquet(res, savePath, saveMode)
    }
  }

  def processAdd4pushData(prevDate: String, curDate: String, saveMode: String, clickIncr: DataFrame) = {
    val ad4pushCurPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.EXTRAS, DataSets.AD4PUSH_ID, DataSets.FULL_MERGE_MODE, curDate)
    if (DataWriter.canWrite(saveMode, ad4pushCurPath)) {
      val ad4pushPrev = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.AD4PUSH_ID, DataSets.FULL_MERGE_MODE, prevDate)
      val ad4pushFull = getAd4pushId(ad4pushPrev, clickIncr)
      DataWriter.writeParquet(ad4pushFull, ad4pushCurPath, saveMode)
    }
  }

  def getAd4pushId(prevFull: DataFrame, clickstreamIncr: DataFrame): DataFrame = {
    val notNullAdd4push = clickstreamIncr
      .select(
        PageVisitVariables.BROWSER_ID,
        PageVisitVariables.ADD4PUSH,
        PageVisitVariables.PAGE_TIMESTAMP
      )
      .dropDuplicates()
      .na.drop(Array(PageVisitVariables.ADD4PUSH))
    val grouped = notNullAdd4push.orderBy(col(PageVisitVariables.BROWSER_ID), desc(PageVisitVariables.PAGE_TIMESTAMP))
      .groupBy(PageVisitVariables.BROWSER_ID)
      .agg(
        first(PageVisitVariables.ADD4PUSH) as PageVisitVariables.ADD4PUSH,
        first(PageVisitVariables.PAGE_TIMESTAMP) as PageVisitVariables.PAGE_TIMESTAMP
      )
    var res: DataFrame = grouped
    if (null != prevFull) {
      res = prevFull.join(grouped, prevFull(PageVisitVariables.BROWSER_ID) === grouped(PageVisitVariables.BROWSER_ID), SQL.FULL_OUTER)
        .select(
          coalesce(prevFull(PageVisitVariables.BROWSER_ID), grouped(PageVisitVariables.BROWSER_ID)) as PageVisitVariables.BROWSER_ID,
          coalesce(grouped(PageVisitVariables.ADD4PUSH), prevFull(PageVisitVariables.ADD4PUSH)) as PageVisitVariables.ADD4PUSH,
          coalesce(grouped(PageVisitVariables.PAGE_TIMESTAMP), prevFull(PageVisitVariables.PAGE_TIMESTAMP)) as PageVisitVariables.PAGE_TIMESTAMP)
    }
    return res
  }

  /**
   *
   * @param path for the dcf csv file
   * @return DataFrame
   */
  def getDataFrameCsv4mDCF(path: String): DataFrame = {
    try {
      val df = DataReader.getDataFrame4mCsv(path, "true", ";")
        .select(
          col("RESPONSYS_ID") as CustomerVariables.RESPONSYS_ID,
          col("CUSTOMER_ID").cast(LongType) as CustomerVariables.ID_CUSTOMER,
          Udf.populateEmail(col("EMAIL"), col("BID")) as CustomerVariables.EMAIL,
          col("BID") as PageVisitVariables.BROWSER_ID,
          // Not using constant IOS as in literal its better if it is local and not any global variable.
          when(col("APPTYPE").contains("ios"), lit("ios")).otherwise(col("APPTYPE")) as PageVisitVariables.DOMAIN
        )
      println("Total recs in DCF file initially: ") // + df.count())

      // df.printSchema()
      // df.show(9)

      val dupFile = "/data/output/extras/duplicate/cust_email.csv"
      val duplicate = DataReader.getDataFrame4mCsv(dupFile, "true", ";")
      println("Total recs in duplicate file: ") // + duplicate.count())
      // duplicate.printSchema()
      // duplicate.show(9)

      val NEW_RESPONSYS_ID = MergeUtils.NEW_ + CustomerVariables.RESPONSYS_ID
      val NEW_ID_CUSTOMER = MergeUtils.NEW_ + CustomerVariables.ID_CUSTOMER
      val NEW_EMAIL = MergeUtils.NEW_ + CustomerVariables.EMAIL
      val NEW_BROWSER_ID = MergeUtils.NEW_ + PageVisitVariables.BROWSER_ID
      val NEW_DOMAIN = MergeUtils.NEW_ + PageVisitVariables.DOMAIN

      val correctRecs = df.join(duplicate, df(CustomerVariables.ID_CUSTOMER) === duplicate(CustomerVariables.ID_CUSTOMER) && df(CustomerVariables.EMAIL) === duplicate(CustomerVariables.EMAIL))
        .select(
          df(CustomerVariables.RESPONSYS_ID) as NEW_RESPONSYS_ID,
          df(CustomerVariables.ID_CUSTOMER) as NEW_ID_CUSTOMER,
          df(CustomerVariables.EMAIL) as NEW_EMAIL,
          df(PageVisitVariables.BROWSER_ID) as NEW_BROWSER_ID,
          df(PageVisitVariables.DOMAIN) as NEW_DOMAIN
        )
      println("Total recs corrected: ") // + correctRecs.count())
      // correctRecs.printSchema()
      // correctRecs.show(9)

      val res = df.join(correctRecs, df(CustomerVariables.ID_CUSTOMER) === correctRecs(NEW_ID_CUSTOMER), SQL.LEFT_OUTER)
        .select(
          coalesce(correctRecs(NEW_RESPONSYS_ID), df(CustomerVariables.RESPONSYS_ID)) as CustomerVariables.RESPONSYS_ID,
          coalesce(correctRecs(NEW_ID_CUSTOMER), df(CustomerVariables.ID_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
          coalesce(correctRecs(NEW_EMAIL), df(CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,
          coalesce(correctRecs(NEW_BROWSER_ID), df(PageVisitVariables.BROWSER_ID)) as PageVisitVariables.BROWSER_ID,
          coalesce(correctRecs(NEW_DOMAIN), df(PageVisitVariables.DOMAIN)) as PageVisitVariables.DOMAIN
        ).dropDuplicates()

      println("Total recs after correction: ") // + res.count())
      // res.printSchema()
      // res.show(9)

      return res

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
