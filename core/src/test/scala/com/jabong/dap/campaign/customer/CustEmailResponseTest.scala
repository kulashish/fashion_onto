
package com.jabong.dap.campaign.customer

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ContactListMobileVars, NewsletterVariables, EmailResponseVariables }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.model.customer.campaigndata.CustEmailResponse
import com.jabong.dap.model.customer.campaigndata.CustEmailResponse._
import com.jabong.dap.model.customer.schema.CustEmailSchema
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.FlatSpec

/**
 * Created by samathashetty on 15/10/15.
 */
class CustEmailResponseTest extends FlatSpec with SharedSparkContext {

  "getDataFrame: Data Frame" should "match with expected data" in {

    val dfClickData = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.CLICK,
      DataSets.DAILY_MODE, "2015/10/09", "53699_CLICK_20151009.txt", "true", ";")
    assert(dfClickData != null)
    assert(dfClickData.count() == 9)
    val aggClickData = reduce(dfClickData, EmailResponseVariables.LAST_CLICK_DATE, EmailResponseVariables.CLICKS_TODAY)
    assert(aggClickData != null)
    assert(aggClickData.count() == 8)
    val selDf = aggClickData.where(col(EmailResponseVariables.CUSTOMER_ID) === "EC8665839ECF5F09E0430100007FC51A").first()
    // last click date
    assert(selDf.get(1).toString.equalsIgnoreCase("08-Oct-2015 23:31:33"))
    // click counts
    assert(selDf.get(2).toString.equalsIgnoreCase("2"))

    val dfOpenData = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.OPEN,
      DataSets.DAILY_MODE, "2015/10/09", "53699_OPEN_20151009.txt", "true", ";")

    val aggOpenData = reduce(dfOpenData, EmailResponseVariables.LAST_OPEN_DATE, EmailResponseVariables.OPENS_TODAY)
    assert(aggOpenData != null)
    assert(aggOpenData.count() == 8)
    val selOpenDf = aggOpenData.where(col(EmailResponseVariables.CUSTOMER_ID) === "EC8665839ECF5F09E0430100007FC51A").first()
    // last click date
    assert(selOpenDf.get(1).toString.equalsIgnoreCase("08-Oct-2015 23:31:29"))
    // click counts
    assert(selOpenDf.get(2).toString.equalsIgnoreCase("2"))

    val outputCsvFormat = udf((s: String) => TimeUtils.changeDateFormat(s: String, TimeConstants.DD_MMM_YYYY_HH_MM_SS, TimeConstants.DATE_TIME_FORMAT))

    val joinedDf = MergeUtils.joinOldAndNewDF(aggClickData, CustEmailSchema.effectiveSchema,
      aggOpenData, CustEmailSchema.effectiveSchema, EmailResponseVariables.CUSTOMER_ID, EmailResponseVariables.CUSTOMER_ID)
      .select(coalesce(col(EmailResponseVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID)) as ContactListMobileVars.UID,
        outputCsvFormat(col(EmailResponseVariables.LAST_OPEN_DATE)) as EmailResponseVariables.LAST_OPEN_DATE,
        col(EmailResponseVariables.OPENS_TODAY).cast(IntegerType) as EmailResponseVariables.OPENS_TODAY,
        col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY).cast(IntegerType) as EmailResponseVariables.CLICKS_TODAY,
        outputCsvFormat(col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE)) as EmailResponseVariables.LAST_CLICK_DATE)
      .withColumn(EmailResponseVariables.OPENS_TODAY, findOpen(col(EmailResponseVariables.OPENS_TODAY), col(EmailResponseVariables.CLICKS_TODAY))).cache()

    print(joinedDf.first().get(joinedDf.first().fieldIndex("UID") ))
    assert(joinedDf.count() == 15)

  }

  "testReadDataFrame" should "match with expected data" in {

    val dfMap = readDF("2015/10/09", "2015/10/08", null)

  }

  "testMergeAllDataFrame" should "match expected values" in {
    val incremental = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "today_incr_csv", CustEmailSchema.todayDf)
    val yesterdayDf = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "yes_full_email", CustEmailSchema.resCustomerEmail)
    val effective7 = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "effective7_email", CustEmailSchema.reqCsvDf)
    val effective15 = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "effective15_email", CustEmailSchema.reqCsvDf)
    val effective30 = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "effective30_email", CustEmailSchema.reqCsvDf)
    val effectiveDFFull = CustEmailResponse.effectiveDFFull(incremental, yesterdayDf, effective7, effective15, effective30)
    val expectedDF = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "expected_result_all", CustEmailSchema.effective_Smry_Schema)
    assert(expectedDF.collect().toSet.equals(effectiveDFFull.collect().toSet))
  }

  "testMergeTodayWithEmpty7Df" should "match expected values" in {
    val incremental = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "today_incr_csv", CustEmailSchema.todayDf)
    val yesterdayDf = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "yes_full_email", CustEmailSchema.resCustomerEmail)
    val effective7 = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], CustEmailSchema.reqCsvDf)
    val effective15 = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "effective15_email", CustEmailSchema.reqCsvDf)
    val effective30 = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "effective30_email", CustEmailSchema.reqCsvDf)
    val effectiveDFFull = CustEmailResponse.effectiveDFFull(incremental, yesterdayDf, effective7, effective15, effective30)
    val expectedDF = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "expected_res_wo7", CustEmailSchema.effective_Smry_Schema)
    assert(expectedDF.collect().toSet.equals(effectiveDFFull.collect().toSet))
  }

  "testMergeEffectiveDfWithCmrAndNl" should "match expected values" in {
    val effectiveDf = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "effective", CustEmailSchema.effective_Smry_Schema)
    val cmr = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "cmr").filter(col(CustomerVariables.EMAIL).isNotNull).filter(col(ContactListMobileVars.UID) isNotNull)
    val nl = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "newsletter_subscription")
    val result = merge(effectiveDf, cmr, nl)
    assert(result.count() === 4)

    readDF("2015/11/09", "2015/10/18", null)
    val udfOpenSegFn = udf((s: String, s1: String, s2: String) => open_segment(s: String, s1: String, s2: String, date))

    val custEmailResCsv = result.select(
      col(ContactListMobileVars.UID),
      udfOpenSegFn(col(EmailResponseVariables.LAST_OPEN_DATE), col(NewsletterVariables.UPDATED_AT),
        col(EmailResponseVariables.END_DATE)),
      col(EmailResponseVariables.OPEN_7DAYS),
      col(EmailResponseVariables.OPEN_15DAYS),
      col(EmailResponseVariables.OPEN_30DAYS),
      col(EmailResponseVariables.CLICK_7DAYS),
      col(EmailResponseVariables.CLICK_15DAYS),
      col(EmailResponseVariables.CLICK_30DAYS),
      col(EmailResponseVariables.LAST_OPEN_DATE),
      col(EmailResponseVariables.LAST_CLICK_DATE),
      col(EmailResponseVariables.OPENS_LIFETIME),
      col(EmailResponseVariables.CLICKS_LIFETIME))

    println(custEmailResCsv)

  }

  "testMergeTodayWithEmpty7_15Df" should "match expected values" in {
    val incremental = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "today_incr_csv", CustEmailSchema.todayDf)
    val yesterdayDf = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "yes_full_email", CustEmailSchema.resCustomerEmail)
    val prevEffDf = SchemaUtils.addColumns(yesterdayDf, CustEmailSchema.effective_Smry_Schema)

    val effective7 = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], CustEmailSchema.reqCsvDf)
    val effective15 = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], CustEmailSchema.reqCsvDf)
    val effective30 = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "effective30_email", CustEmailSchema.reqCsvDf)
    val effectiveDFFull = CustEmailResponse.effectiveDFFull(incremental, prevEffDf, effective7, effective15, effective30)
    //    val expectedDF = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "expected_res_wo7_15", CustEmailSchema.effective_Smry_Schema)
    //    assert(expectedDF.collect().toSet.equals(effectiveDFFull.collect().toSet))

    val cmr = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "cmr")
    val nl = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "newsletter_subscription")
    val result = merge(effectiveDFFull, cmr, nl)

  }

  "testMergeTodayWithEmpty7_15_30" should "match expected values" in {
    val incremental = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "today_incr_csv", CustEmailSchema.todayDf)
    val yesterdayDf = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "yes_full_email", CustEmailSchema.resCustomerEmail)
    //    val yesTodayDf = CustEmailResponse.joinDataFrames(incremental, yesterdayDf)
    val effective7 = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], CustEmailSchema.todayDf)
    val effective15 = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], CustEmailSchema.todayDf)
    val effective30 = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], CustEmailSchema.todayDf)
    val effectiveDFFull = CustEmailResponse.effectiveDFFull(incremental, yesterdayDf, effective7, effective15, effective30)
    val expectedDF = JsonUtils.readFromJson(DataSets.CUST_EMAIL_RESPONSE, "expected_res_wo_any", CustEmailSchema.effective_Smry_Schema)
    assert(expectedDF.collect().toSet.equals(effectiveDFFull.collect().toSet))
  }

}
