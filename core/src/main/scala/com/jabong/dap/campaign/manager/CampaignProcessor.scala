package com.jabong.dap.campaign.manager

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.data.read.DataReader
import org.apache.spark.sql.DataFrame
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import org.apache.spark.sql.functions._

/**
 * Created by Mubarak on 28/7/15.
 */
class CampaignProcessor {

  def mapDeviceFromDCF(dcf: DataFrame, campaign: DataFrame, key: String): DataFrame={
    val notNull = campaign.na.drop(Array(
      CampaignMergedFields.FK_CUSTOMER,
      CampaignMergedFields.DEVICE_ID
    ))
    var key1: String = null
    if (key.equals(CampaignMergedFields.FK_CUSTOMER)){
      key1 = CustomerVariables.ID_CUSTOMER
    }
    else{
      key1 = key
    }
    val dcfn = dcf.filter(!dcf(CampaignMergedFields.DEVICE_ID)===null)
    val bcCampaign = Spark.getContext().broadcast(notNull).value
    val campaignDevice = dcfn.join(bcCampaign,bcCampaign(key)===dcfn(key1))
    .select(bcCampaign(CampaignMergedFields.FK_CUSTOMER) as CampaignMergedFields.FK_CUSTOMER,
        bcCampaign(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        bcCampaign(CampaignMergedFields.REF_SKU1),
        coalesce(bcCampaign(CampaignMergedFields.DEVICE_ID), dcfn(CampaignMergedFields.DEVICE_ID)),
        coalesce(bcCampaign(CampaignMergedFields.EMAIL), dcfn(CampaignMergedFields.EMAIL)),
        coalesce(bcCampaign(CampaignMergedFields.DOMAIN), dcfn(CampaignMergedFields.DOMAIN))
      )
    campaignDevice
  }

  def splitCampaigns(surfCampaign: DataFrame, campaign: DataFrame): DataFrame ={

    var joinedDF = surfCampaign.unionAll(campaign)

    val custIdNUll = joinedDF.filter(joinedDF(CampaignMergedFields.FK_CUSTOMER) === null)

    val custIdNotNUll = joinedDF.filter(!joinedDF(CampaignMergedFields.FK_CUSTOMER) === null)

    val custId = CampaignManager.campaignMerger(custIdNotNUll,CampaignMergedFields.FK_CUSTOMER)

    val DeviceId = CampaignManager.campaignMerger(custIdNotNUll,CampaignMergedFields.DEVICE_ID)

    custId.unionAll(DeviceId)

  }

}
