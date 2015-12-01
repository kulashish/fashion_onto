package com.jabong.dap.model.customer.data

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, PageVisitVariables }
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.common.{ GroupedUtils, OptionUtils, Spark }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.{ DataNotFound, DataReader, ValidFormatNotFound }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.storage.schema.{ Schema, OrderBySchema }
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ LongType, TimestampType }

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
  def getLatestDevice(clickStreamInc: DataFrame, cmr: DataFrame, customer: DataFrame, nlsIncr: DataFrame): DataFrame = {
    // val filData = clickStreamInc.filter(!clickStreamInc(PageVisitVariables.USER_ID).startsWith(CustomerVariables.APP_FILTER))
    println("clickStreamInc: ") // + clickStreamInc.count())
    // clickStreamInc.printSchema()
    // clickStreamInc.show(10)
    val clickStream = clickStreamInc.filter(!(col("userid").startsWith("_app_")))
      .filter(PageVisitVariables.DOMAIN + " IN ('" + DataSets.IOS + "', '" + DataSets.ANDROID + "', '" + DataSets.WINDOWS + "')")

    val groupedFields = Array(PageVisitVariables.USER_ID)
    val aggFields = Array(PageVisitVariables.USER_ID, PageVisitVariables.BROWSER_ID, PageVisitVariables.DOMAIN)

    val latestDeviceData = GroupedUtils.orderGroupBy(clickStream, groupedFields, aggFields, GroupedUtils.FIRST, OrderBySchema.latestDeviceSchema, PageVisitVariables.PAGE_TIMESTAMP, GroupedUtils.DESC, TimestampType)

    println("clickStream after aggregation and filtering: ") // + clickStream.count())
    // clickStream.printSchema()
    // clickStream.show(10)

    // outerjoin with customer table one day increment on userid = email
    // id_customer, email, browser_id, domain
    val custUnq = customer.select(CustomerVariables.ID_CUSTOMER, CustomerVariables.EMAIL).dropDuplicates()

    val nlsUnq = nlsIncr.select(CustomerVariables.FK_CUSTOMER, CustomerVariables.EMAIL).dropDuplicates()
      .filter(col(CustomerVariables.FK_CUSTOMER).equalTo(0) || col(CustomerVariables.FK_CUSTOMER).isNull)

    val nlsJoined = custUnq.join(nlsUnq, custUnq(CustomerVariables.EMAIL) === nlsUnq(CustomerVariables.EMAIL), SQL.FULL_OUTER)
      .select(
        coalesce(custUnq(CustomerVariables.EMAIL), nlsUnq(CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,
        coalesce(custUnq(CustomerVariables.ID_CUSTOMER), nlsUnq(CustomerVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER
      )

    val nlsbc = Spark.getContext().broadcast(nlsJoined).value

    val joinedDf = latestDeviceData.join(nlsbc, nlsbc(CustomerVariables.EMAIL) === latestDeviceData(PageVisitVariables.USER_ID), SQL.FULL_OUTER)
      .select(
        coalesce(nlsbc(CustomerVariables.EMAIL), latestDeviceData(PageVisitVariables.USER_ID)) as CustomerVariables.EMAIL,
        nlsbc(CustomerVariables.ID_CUSTOMER),
        latestDeviceData(PageVisitVariables.BROWSER_ID),
        latestDeviceData(PageVisitVariables.DOMAIN)
      )

    println("After outer join with customer table: ") // + joinedDf.count())
    // joinedDf.printSchema()
    // joinedDf.show(10)

    val joined = joinedDf.join(cmr, cmr(CustomerVariables.EMAIL) === joinedDf(CustomerVariables.EMAIL), SQL.FULL_OUTER)
      .select(
        cmr(ContactListMobileVars.UID),
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

    result
  }

  /**
   *
   * @param vars
   */
  def start(vars: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate))
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
      var nlsIncr: DataFrame = null
      var customerIncr: DataFrame = null
      if (null != path) {
        if ("firstTime4Nls".equals(path)) {
          cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, prevDate)
            .filter(col(CustomerVariables.ID_CUSTOMER).geq(1))
          if (!SchemaUtils.isSchemaEqual(cmrFull.schema, Schema.cmr)) {
            println("correctingSchema")
            cmrFull.printSchema()
            cmrFull = SchemaUtils.changeSchema(cmrFull, Schema.cmr)
            cmrFull.printSchema()
          }
          nlsIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.NEWSLETTER_SUBSCRIPTION, DataSets.FULL_MERGE_MODE, curDate)
          customerIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.FULL_MERGE_MODE, curDate)
        } else if ("dcfUID".equals(path)) {
          val fileDate = TimeUtils.changeDateFormat(curDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
          val cmrFullDCF = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, "dcf", "contact_list", DataSets.FULL_MERGE_MODE,
            prevDate, "CONTACTS_LIST_" + fileDate + ".csv", "true", "|")
            .select(
              col(ContactListMobileVars.UID) as CustomerVariables.UID,
              col(ContactListMobileVars.EMAIL) as CustomerVariables.EMAIL
            ).na.drop(ContactListMobileVars.EMAIL).dropDuplicates()
          println("Total recs in DCF file initially: " + cmrFullDCF.count())
          val cmrPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, prevDate)
          val cmrJoined = cmrPrevFull.join(cmrFullDCF, cmrPrevFull("EMAIL") === cmrFullDCF("EMAIL"))
          cmrFull = cmrJoined.select(
            cmrFullDCF(ContactListMobileVars.UID),
            cmrPrevFull(CustomerVariables.EMAIL),
            cmrPrevFull(CustomerVariables.RESPONSYS_ID),
            cmrPrevFull(CustomerVariables.ID_CUSTOMER),
            cmrPrevFull(PageVisitVariables.BROWSER_ID),
            cmrPrevFull(PageVisitVariables.DOMAIN)
          )
        } else {
          cmrFull = getDataFrameCsv4mDCF(path)
        }
      } else {
        cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, prevDate)
        nlsIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.NEWSLETTER_SUBSCRIPTION, DataSets.DAILY_MODE, curDate)
        customerIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.DAILY_MODE, curDate)
      }

      val res = getLatestDevice(clickIncr, cmrFull, customerIncr, nlsIncr)

      val filledUid = UUIDGenerator.addUid(res)

      DataWriter.writeParquet(filledUid, savePath, saveMode)
      // DataWriter.writeParquet(res, savePath, saveMode)
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
        PageVisitVariables.DOMAIN,
        PageVisitVariables.ADD4PUSH,
        PageVisitVariables.PAGE_TIMESTAMP
      )
      .filter(col(PageVisitVariables.DOMAIN) === DataSets.ANDROID)
      .dropDuplicates()
      .na.drop(Array(PageVisitVariables.ADD4PUSH))

    val groupedFields = Array(PageVisitVariables.BROWSER_ID)
    val aggFields = Array(PageVisitVariables.BROWSER_ID, PageVisitVariables.ADD4PUSH, PageVisitVariables.PAGE_TIMESTAMP)

    val grouped = GroupedUtils.orderGroupBy(notNullAdd4push, groupedFields, aggFields, GroupedUtils.FIRST, OrderBySchema.ad4PushIntermediateSchema, PageVisitVariables.PAGE_TIMESTAMP, GroupedUtils.DESC, TimestampType)

    //    val grouped = notNullAdd4push.orderBy(col(PageVisitVariables.BROWSER_ID), desc(PageVisitVariables.PAGE_TIMESTAMP))
    //      .groupBy(PageVisitVariables.BROWSER_ID)
    //      .agg(
    //        first(PageVisitVariables.ADD4PUSH) as PageVisitVariables.ADD4PUSH,
    //        first(PageVisitVariables.PAGE_TIMESTAMP) as PageVisitVariables.PAGE_TIMESTAMP
    //      )
    var res: DataFrame = grouped
    if (null != prevFull) {
      res = prevFull.join(grouped, prevFull(PageVisitVariables.BROWSER_ID) === grouped(PageVisitVariables.BROWSER_ID), SQL.FULL_OUTER)
        .select(
          coalesce(prevFull(PageVisitVariables.BROWSER_ID), grouped(PageVisitVariables.BROWSER_ID)) as PageVisitVariables.BROWSER_ID,
          coalesce(grouped(PageVisitVariables.ADD4PUSH), prevFull(PageVisitVariables.ADD4PUSH)) as PageVisitVariables.ADD4PUSH,
          coalesce(grouped(PageVisitVariables.PAGE_TIMESTAMP), prevFull(PageVisitVariables.PAGE_TIMESTAMP)) as PageVisitVariables.PAGE_TIMESTAMP)
    }
    res
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
        ).dropDuplicates().filter(col(CustomerVariables.ID_CUSTOMER).isNotNull && col(CustomerVariables.ID_CUSTOMER).cast(LongType).geq(1))

      println("Total recs after correction: ") // + res.count())
      // res.printSchema()
      // res.show(9)

      res

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
