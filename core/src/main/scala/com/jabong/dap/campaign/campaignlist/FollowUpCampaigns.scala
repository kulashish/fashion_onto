package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ContactListMobileVars }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by rahul on 13/10/15.
 */
class FollowUpCampaigns {

  /**
   * follow up campaign for surf1,surf2,surf3,surf6 and cancel,return retarget
   * @param campaignMergedData
   * @param salesOrderLast3Days
   * @param itrSkuYesterdayData
   */
  def runCampaign(campaignMergedData: DataFrame, salesOrderLast3Days: DataFrame, itrSkuYesterdayData: DataFrame, incrDate: String) = {

    val selectedData = CampaignUtils.campaignFollowUpSelection(campaignMergedData, salesOrderLast3Days)

    val refSku1Out = CampaignUtils.campaignSkuStockFilter(selectedData, itrSkuYesterdayData, CampaignMergedFields.LIVE_REF_SKU1, 3)
    val refSku2Out = CampaignUtils.campaignSkuStockFilter(refSku1Out, itrSkuYesterdayData, CampaignMergedFields.LIVE_REF_SKU + "2", 3).cache()
    //    val refSku3Out = CampaignUtils.campaignSkuStockFilter(refSku2Out, itrSkuSimpleYesterdayData, CampaignMergedFields.LIVE_REF_SKU + "3", 3)

    CampaignOutput.saveCampaignData(refSku2Out, CampaignCommon.FOLLOW_UP_CAMPAIGNS, DataSets.EMAIL_CAMPAIGNS, incrDate)

    val dfCSV = refSku2Out.
      select(
        ContactListMobileVars.UID,
        ContactListMobileVars.EMAIL,
        CampaignMergedFields.LIVE_MAIL_TYPE,
        CampaignMergedFields.LIVE_BRAND,
        CampaignMergedFields.LIVE_BRICK,
        CampaignMergedFields.LIVE_PROD_NAME,
        CampaignMergedFields.LIVE_REF_SKU + "1",
        CampaignMergedFields.LIVE_REF_SKU + "2",
        CampaignMergedFields.LIVE_REF_SKU + "3",
        CampaignMergedFields.LIVE_REC_SKU + "1",
        CampaignMergedFields.LIVE_REC_SKU + "2",
        CampaignMergedFields.LIVE_REC_SKU + "3",
        CampaignMergedFields.LIVE_REC_SKU + "4",
        CampaignMergedFields.LIVE_REC_SKU + "5",
        CampaignMergedFields.LIVE_REC_SKU + "6",
        CampaignMergedFields.LIVE_REC_SKU + "7",
        CampaignMergedFields.LIVE_REC_SKU + "8",
        CampaignMergedFields.LIVE_CART_URL,
        CampaignMergedFields.LAST_UPDATED_DATE,
        ContactListMobileVars.MOBILE,
        CampaignMergedFields.TYPO_MOBILE_PERMISION_STATUS,
        CampaignMergedFields.COUNTRY_CODE
      )

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    CampaignOutput.saveCampaignCSVForYesterday(dfCSV, CampaignCommon.FOLLOW_UP_CAMPAIGNS, fileDate + "_live_campaign_followup", DataSets.EMAIL_CAMPAIGNS)
  }
}
