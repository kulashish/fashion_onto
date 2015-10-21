
package com.jabong.dap.campaign.customer

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.EmailResponseVariables
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.model.customer.campaigndata.CustEmailResponse
import com.jabong.dap.model.customer.campaigndata.CustEmailResponse._
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec


/**
 * Created by samathashetty on 15/10/15.
 */
class CustEmailResponseTest  extends FlatSpec with SharedSparkContext {

  "getDataFrame: Data Frame" should "match with expected data" in {

    val dfClickData = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.CLICK,
      DataSets.DAILY_MODE, "2015/10/09", "53699_CLICK_20151009.txt", "true", ";")
    assert(dfClickData != null)
    assert(dfClickData.count() == 9)
    val aggClickData = reduce(dfClickData, EmailResponseVariables.LAST_CLICK_DATE, EmailResponseVariables.CLICKS_TODAY)
    assert(aggClickData != null)
    assert(aggClickData.count() == 8)
    val selDf = aggClickData.where(col(EmailResponseVariables.CUSTOMER_ID) ===  "EC8665839ECF5F09E0430100007FC51A").first()
    // last click date
    assert(selDf.get(1).toString.equalsIgnoreCase("08-Oct-2015 23:31:33"))
    // click counts
    assert(selDf.get(2).toString.equalsIgnoreCase("2"))


    val dfOpenData = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.OPEN,
      DataSets.DAILY_MODE, "2015/10/10", "53699_OPEN_20151009.txt", "true", ";")

    val aggOpenData = reduce(dfOpenData, EmailResponseVariables.LAST_OPEN_DATE, EmailResponseVariables.OPENS_TODAY)
    assert(aggOpenData != null)
    assert(aggOpenData.count() == 8)
    val selOpenDf = aggOpenData.where(col(EmailResponseVariables.CUSTOMER_ID) ===  "EC8665839ECF5F09E0430100007FC51A").first()
    // last click date
    assert(selOpenDf.get(1).toString.equalsIgnoreCase("08-Oct-2015 23:31:29"))
    // click counts
    assert(selOpenDf.get(2).toString.equalsIgnoreCase("2"))

   val joinedDf = MergeUtils.joinOldAndNewDF(aggClickData, aggOpenData, EmailResponseVariables.CUSTOMER_ID)
     .select(coalesce(col(EmailResponseVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID)) as EmailResponseVariables.CUSTOMER_ID,
     col(EmailResponseVariables.LAST_OPEN_DATE) as EmailResponseVariables.LAST_OPEN_DATE,
       col(EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPENS_TODAY,
       col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICKS_TODAY,
       col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE) as EmailResponseVariables.LAST_CLICK_DATE)

    assert(joinedDf.count() == 15)


  }


  "testReadDataFrame" should "match with expected data" in {
    //val joinedDf = readDataFrame("2015-10-09", DataSets.DAILY_MODE)
    CustEmailResponse.emailResponse("2015-10-09", DataSets.OVERWRITE_SAVEMODE, null)
    //val expectedDf = DataReader.getDataFrame4mCsv(JsonUtils.TEST_RESOURCES, DataSets.CUSTOMER, DataSets.CSV, DataSets.DAILY_MODE, "2015/10/10", "53699_CLICK_20151009.txt", "true", ";")
    //assert(joinedDf === expectedDf)
  }
}
