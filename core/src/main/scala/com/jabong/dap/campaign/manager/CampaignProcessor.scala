package com.jabong.dap.campaign.manager

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.CustomerVariables
import org.apache.spark.sql.DataFrame
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import org.apache.spark.sql.functions._

/**
 * Created by Mubarak on 28/7/15.
 */
class CampaignProcessor {

  def mapDeviceFromDCF(dcf: DataFrame, campaign: DataFrame, key: String): DataFrame={
    val notNull = campaign.na.drop(Array(
      CampaignMergedFields.CUSTOMER_ID,
      CampaignMergedFields.DEVICE_ID
    ))
    var key1: String = null
    if (key.equals(CampaignMergedFields.CUSTOMER_ID)){
      key1 = CustomerVariables.ID_CUSTOMER
    }
    else{
      key1 = key
    }
    val dcfn = dcf.filter(!dcf(CampaignMergedFields.DEVICE_ID)===null)
    val bcCampaign = Spark.getContext().broadcast(notNull).value
    val campaignDevice = dcfn.join(bcCampaign,bcCampaign(key)===dcfn(key1))
    .select(bcCampaign(CampaignMergedFields.CUSTOMER_ID) as CampaignMergedFields.CUSTOMER_ID,
        bcCampaign(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        bcCampaign(CampaignMergedFields.REF_SKU1),
        coalesce(bcCampaign(CampaignMergedFields.DEVICE_ID), dcfn(CampaignMergedFields.DEVICE_ID)),
        coalesce(bcCampaign(CampaignMergedFields.EMAIL), dcfn(CampaignMergedFields.EMAIL)),
        coalesce(bcCampaign(CampaignMergedFields.DOMAIN), dcfn(CampaignMergedFields.DOMAIN))
      )
    campaignDevice
  }

  def splitNMergeCampaigns(surfCampaign: DataFrame, campaign: DataFrame): DataFrame ={

    var joinedDF = surfCampaign.unionAll(campaign)

    val custIdNUll = joinedDF.filter(joinedDF(CampaignMergedFields.CUSTOMER_ID) === null)

    val custIdNotNUll = joinedDF.filter(!joinedDF(CampaignMergedFields.CUSTOMER_ID) === null)

    val custId = CampaignManager.campaignMerger(custIdNotNUll, CampaignMergedFields.CUSTOMER_ID, CampaignMergedFields.DEVICE_ID)

    val DeviceId = CampaignManager.campaignMerger(custIdNotNUll, CampaignMergedFields.DEVICE_ID, CampaignMergedFields.CUSTOMER_ID)

    custId.unionAll(DeviceId)

  }

}
