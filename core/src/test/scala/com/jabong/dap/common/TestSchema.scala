package com.jabong.dap.common

import com.jabong.dap.common.constants.campaign.{ Recommendation, CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.constants.variables._
import org.apache.spark.sql.types._

/**
 * Created by pooja on 27/8/15.
 */
object TestSchema {

  val resultFullSkuFilter = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(ProductVariables.SKU_SIMPLE, StringType, true),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true)
  ))

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer_segments variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val mvp_seg = StructType(Array(
    StructField(CustomerSegmentsVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerSegmentsVariables.MVP_TYPE, IntegerType, true),
    StructField(CustomerSegmentsVariables.SEGMENT, IntegerType, true),
    StructField(CustomerSegmentsVariables.DISCOUNT_SCORE, IntegerType, true)
  ))

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer_storecredits_history variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val last_jr_covert_date = StructType(Array(
    StructField(CustomerStoreVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerStoreVariables.LAST_JR_COVERT_DATE, TimestampType, true)
  ))

  val accRegDateAndUpdatedAt = StructType(Array(
    StructField(CustomerVariables.EMAIL, StringType, true),
    StructField(CustomerVariables.REG_DATE, TimestampType, true),
    StructField(CustomerVariables.UPDATED_AT, TimestampType, true)
  ))

  val emailOptInStatus = StructType(Array(
    StructField(CustomerVariables.ID_CUSTOMER, LongType, true),
    StructField(NewsletterVariables.STATUS, StringType, true)
  ))

  val resultCustomerProductShortlist = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(CustomerProductShortlistVariables.SKU, StringType, true),
    StructField(CustomerProductShortlistVariables.DOMAIN, StringType, true),
    StructField(CustomerProductShortlistVariables.USER_DEVICE_TYPE, StringType, true),
    StructField(CustomerProductShortlistVariables.CREATED_AT, TimestampType, true),
    StructField(CustomerProductShortlistVariables.SKU_SIMPLE, StringType, true),
    StructField(CustomerProductShortlistVariables.PRICE, DecimalType(10, 2), true)
  ))

  val resultGetJoin = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(CustomerProductShortlistVariables.SKU_SIMPLE, StringType, true),
    StructField(ItrVariables.SPECIAL_PRICE, DecimalType(10, 2), true)
  ))

  val resultSkuSimpleFilter = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(CustomerProductShortlistVariables.SKU_SIMPLE, StringType, true),
    StructField(CustomerProductShortlistVariables.SPECIAL_PRICE, DecimalType(10, 2), true)
  ))

  val resultSkuFilter = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(CustomerProductShortlistVariables.SKU, StringType, true),
    StructField(CustomerProductShortlistVariables.AVERAGE_PRICE, DecimalType(10, 2), true)
  ))

  val campaignOutput = StructType(Array(
    StructField(CampaignMergedFields.CUSTOMER_ID, LongType, true),
    StructField(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, IntegerType, true),
    StructField(CampaignMergedFields.REF_SKU1, StringType, true),
    StructField(CampaignMergedFields.REF_SKU2, StringType, true)
  ))

  val campaignPriorityOutput = StructType(Array(
    StructField(CampaignMergedFields.CUSTOMER_ID, LongType, true),
    StructField(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, IntegerType, true),
    StructField(CampaignMergedFields.REF_SKU1, StringType, true),
    StructField(CampaignMergedFields.EMAIL, StringType, true),
    StructField(CampaignMergedFields.DOMAIN, StringType, true),
    StructField(CampaignMergedFields.DEVICE_ID, StringType, true),
    StructField(CampaignCommon.PRIORITY, IntegerType, true)
  ))

  val customerPageVisitSkuListLevel = StructType(Array(
    StructField(PageVisitVariables.USER_ID, StringType, true),
    StructField(PageVisitVariables.BROWSER_ID, StringType, true),
    StructField(PageVisitVariables.ACTUAL_VISIT_ID, StringType, true),
    StructField(PageVisitVariables.DOMAIN, StringType, true),
    StructField(PageVisitVariables.SKU_LIST, ArrayType(StringType), true)
  ))

  val customerPageVisitSkuLevel = StructType(Array(
    StructField(PageVisitVariables.USER_ID, StringType, true),
    StructField(PageVisitVariables.BROWSER_ID, StringType, true),
    StructField(PageVisitVariables.ACTUAL_VISIT_ID, StringType, true),
    StructField(PageVisitVariables.DOMAIN, StringType, true),
    StructField(PageVisitVariables.SKU, StringType, true)
  ))

  val customerDeviceMapping = StructType(Array(
    StructField(CustomerVariables.EMAIL, StringType, true),
    StructField(CustomerVariables.RESPONSYS_ID, StringType, true),
    StructField(CustomerVariables.ID_CUSTOMER, LongType, true),
    StructField(PageVisitVariables.BROWSER_ID, StringType, true),
    StructField(PageVisitVariables.DOMAIN, StringType, true)
  ))

  val inventoryCheckInput = StructType(Array(
    StructField(Recommendation.SALES_ORDER_ITEM_SKU, StringType, true),
    StructField(ProductVariables.BRICK, StringType, true),
    StructField(ProductVariables.MVP, StringType, true),
    StructField(ProductVariables.BRAND, StringType, true),
    StructField(ProductVariables.CATEGORY, StringType, true),
    StructField(ProductVariables.GENDER, StringType, true),
    StructField(ProductVariables.NUMBER_SIMPLE_PER_SKU, LongType, true),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true),
    StructField(ProductVariables.STOCK, LongType, true),
    StructField(Recommendation.NUMBER_LAST_30_DAYS_ORDERED, LongType, true),
    StructField(Recommendation.WEEKLY_AVERAGE_SALE, DoubleType, true),
    StructField(Recommendation.LAST_SOLD_DATE, TimestampType, true)
  ))

  val skuCompleteInput = StructType(Array(
    StructField(Recommendation.SALES_ORDER_ITEM_SKU, StringType, true),
    StructField(Recommendation.NUMBER_LAST_30_DAYS_ORDERED, LongType, true),
    StructField(Recommendation.WEEKLY_AVERAGE_SALE, DoubleType, true),
    StructField(Recommendation.LAST_SOLD_DATE, TimestampType, true)
  ))

  val basicItr = StructType(Array(
    StructField(ProductVariables.SKU, StringType, true),
    StructField(ProductVariables.BRICK, StringType, true),
    StructField(ProductVariables.MVP, StringType, true),
    StructField(ProductVariables.BRAND, StringType, true),
    StructField(ProductVariables.CATEGORY, StringType, true),
    StructField(ProductVariables.GENDER, StringType, true),
    StructField(ProductVariables.NUMBER_SIMPLE_PER_SKU, LongType, true),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true),
    StructField(ProductVariables.STOCK, LongType, true)
  ))

  val recommendationSku = StructType(Array(
    StructField(Recommendation.SALES_ORDER_ITEM_SKU, StringType, true),
    StructField(ProductVariables.BRICK, StringType, true),
    StructField(ProductVariables.MVP, StringType, true),
    StructField(ProductVariables.BRAND, StringType, true),
    StructField(ProductVariables.GENDER, StringType, true),
    StructField(Recommendation.NUMBER_LAST_30_DAYS_ORDERED, LongType, true),
    StructField(TestConstants.TEST_CASE_FILTER, LongType, true)
  ))

  val referenceSku = StructType(Array(
    StructField(CustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(ProductVariables.BRICK, StringType, true),
    StructField(ProductVariables.MVP, StringType, true),
    StructField(ProductVariables.GENDER, StringType, true),
    StructField(CampaignMergedFields.REF_SKU1, StringType, false),
    StructField(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, IntegerType, true)
  ))

  val finalReferenceSku = StructType(Array(
    StructField(CustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(CampaignMergedFields.REF_SKU1, StringType, false),
    StructField(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, IntegerType, true),
    StructField(CampaignMergedFields.REF_SKUS, ArrayType(StructType(Array(StructField(ProductVariables.BRICK, StringType, true),
      StructField(ProductVariables.MVP, StringType, true),StructField(CampaignMergedFields.REF_SKU, StringType, true),StructField(ProductVariables.GENDER, StringType, true)))), false)))

  val genRecInput =  StructType(Array(
    StructField(CustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(CampaignMergedFields.REF_SKU, StringType, false),
    StructField(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, IntegerType, true),
    StructField(CampaignMergedFields.REC_SKUS,ArrayType(StringType), false),
    StructField(TestConstants.TEST_CASE_FILTER, LongType, true)))

  val refSkuInput =  StructType(Array(
    StructField(CustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(ProductVariables.SKU_SIMPLE, StringType, false),
    StructField(ProductVariables.SPECIAL_PRICE,DecimalType(10, 2), true),
    StructField(ProductVariables.MVP,StringType, false),
    StructField(ProductVariables.GENDER, StringType, true),
    StructField(ProductVariables.BRICK, StringType, true),
    StructField(ProductVariables.BRAND, StringType, true)))

  val salesOrderPaybackEarn = StructType(Array(StructField(PaybackCustomerVariables.FK_SALES_ORDER, IntegerType, true)))

  val salesOrderPaybackRedeem = StructType(Array(StructField(PaybackCustomerVariables.FK_CUSTOMER, LongType, true)))

  val paybackCustomer = StructType(Array(StructField(PaybackCustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(PaybackCustomerVariables.IS_PAYBACK, BooleanType, true)))

}
