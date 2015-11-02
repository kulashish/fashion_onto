package com.jabong.dap.data.storage.schema

import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.{ PageVisitVariables, ContactListMobileVars, CustomerVariables }
import org.apache.spark.sql.types._

/**
 * Created by rahul on 2/11/15.
 */
object OrderBySchema {

  val pushCampaignSchema = StructType(Array(
    StructField(CampaignMergedFields.DEVICE_ID, StringType, true),
    StructField(CampaignMergedFields.CUSTOMER_ID, LongType, true),
    StructField(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, IntegerType, true),
    StructField(CampaignMergedFields.REF_SKU1, StringType, true),
    StructField(CampaignMergedFields.EMAIL, StringType, true),
    StructField(CampaignMergedFields.DOMAIN, StringType, true)

  ))

  val emailCampaignSchema = StructType(Array(
    StructField(CustomerVariables.EMAIL, StringType, false),
    StructField(ContactListMobileVars.UID, StringType, true),
    StructField(CampaignMergedFields.CUSTOMER_ID, LongType, true),
    StructField(CampaignMergedFields.REF_SKUS, ArrayType(StringType), true),
    StructField(CampaignMergedFields.REC_SKUS, ArrayType(StringType), true),
    StructField(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, StringType, true),
    StructField(CampaignMergedFields.LIVE_CART_URL, StringType, true)
  ))

  val latestDeviceSchema = StructType(Array(
    StructField(PageVisitVariables.USER_ID, StringType, true),
    StructField(PageVisitVariables.BROWSER_ID, StringType, true),
    StructField(CampaignMergedFields.CUSTOMER_ID, LongType, true)
  ))

  val ad4PushIntermediateSchema = StructType(Array(
    StructField(PageVisitVariables.BROWSER_ID, StringType, true),
    StructField(PageVisitVariables.ADD4PUSH, StringType, true),
    StructField(CampaignMergedFields.CUSTOMER_ID, LongType, true)
  ))
}
