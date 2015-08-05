package com.jabong.dap.campaign.manager

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CampaignMergedFields}
import com.jabong.dap.common.constants.variables.{CustomerVariables, PageVisitVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, LongType}

/**
 * Created by Mubarak on 28/7/15.
 */
object CampaignProcessor {

  def mapDeviceFromCMR(cmr: DataFrame, campaign: DataFrame, key: String): DataFrame = {
    val notNullCampaign = campaign.na.drop("all", Array(
      CampaignMergedFields.CUSTOMER_ID,
      CampaignMergedFields.DEVICE_ID
    ))
    var key1: String = null
    if (key.equals(CampaignMergedFields.CUSTOMER_ID)) {
      key1 = CustomerVariables.ID_CUSTOMER
    } else {
      key1 = key
    }

    val cmrn = cmr.na.drop(Array(PageVisitVariables.BROWSER_ID))
      .select(
        cmr(CustomerVariables.EMAIL),
        cmr(CustomerVariables.RESPONSYS_ID),
        cmr(CustomerVariables.ID_CUSTOMER).cast(LongType) as CustomerVariables.ID_CUSTOMER,
        cmr(PageVisitVariables.BROWSER_ID),
        cmr(PageVisitVariables.DOMAIN)
      )

    val bcCampaign = Spark.getContext().broadcast(notNullCampaign).value
    val campaignDevice = cmrn.join(bcCampaign, bcCampaign(key) === cmrn(key1))
      .select(
        bcCampaign(CampaignMergedFields.CUSTOMER_ID) as CampaignMergedFields.CUSTOMER_ID,
        bcCampaign(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        bcCampaign(CampaignMergedFields.REF_SKU1),
        bcCampaign(CampaignCommon.PRIORITY),
        coalesce(bcCampaign(CampaignMergedFields.DEVICE_ID), cmrn(PageVisitVariables.BROWSER_ID)) as CampaignMergedFields.DEVICE_ID,
        coalesce(bcCampaign(CampaignMergedFields.EMAIL), cmrn(CampaignMergedFields.EMAIL)) as CampaignMergedFields.EMAIL,
        coalesce(bcCampaign(CampaignMergedFields.DOMAIN), cmrn(CampaignMergedFields.DOMAIN)) as CampaignMergedFields.DOMAIN
      )
    campaignDevice
  }

  def splitNMergeCampaigns(campaign: DataFrame, itr: DataFrame): DataFrame = {

    val custIdNUll = campaign.filter(campaign(CampaignMergedFields.CUSTOMER_ID) === 0)

    val custIdNotNUll = campaign.filter(!campaign(CampaignMergedFields.CUSTOMER_ID) === 0)

    val custId = CampaignManager.campaignMerger(custIdNotNUll, CampaignMergedFields.CUSTOMER_ID, CampaignMergedFields.DEVICE_ID)

    val DeviceId = CampaignManager.campaignMerger(custIdNUll, CampaignMergedFields.DEVICE_ID, CampaignMergedFields.CUSTOMER_ID)

    val camp = custId.unionAll(DeviceId)
    val yesterdayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT) //YYYY-MM-DD

    val finalCampaign = camp.join(itr, camp(CampaignMergedFields.REF_SKU1) === itr(ITR.CONFIG_SKU))
      .select(
        camp(CampaignMergedFields.CUSTOMER_ID) as CampaignMergedFields.CUSTOMER_ID,
        camp(CampaignMergedFields.CAMPAIGN_MAIL_TYPE) as CampaignMergedFields.LIVE_MAIL_TYPE,
        camp(CampaignMergedFields.REF_SKU1) as CampaignMergedFields.LIVE_REF_SKU1,
        camp(CampaignMergedFields.EMAIL) as CampaignMergedFields.EMAIL,
        camp(CampaignMergedFields.DOMAIN) as CampaignMergedFields.DOMAIN,
        camp(CampaignMergedFields.DEVICE_ID) as CampaignMergedFields.deviceId,
        camp(CampaignMergedFields.LIVE_CART_URL) as CampaignMergedFields.LIVE_CART_URL,
        itr(ITR.PRODUCT_NAME) as CampaignMergedFields.LIVE_PROD_NAME,
        itr(ITR.BRAND_NAME) as CampaignMergedFields.LIVE_BRAND,
        itr(ITR.BRICK) as CampaignMergedFields.LIVE_BRICK,
     // lit("www.jabong.com/cart/addmulti?skus="+camp(CampaignMergedFields.REF_SKU1)).cast(StringType) as CampaignMergedFields.LIVE_CART_URL,
        lit(yesterdayDate).cast(StringType) as CampaignMergedFields.END_OF_DATE
      )

    finalCampaign
  }

}
