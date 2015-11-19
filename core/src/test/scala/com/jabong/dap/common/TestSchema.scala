package com.jabong.dap.common

import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields, Recommendation }
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.model.clickstream.campaignData.CustomerAppDetails
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
    StructField(ContactListMobileVars.MVP_TYPE, IntegerType, true),
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
    StructField(ContactListMobileVars.REG_DATE, TimestampType, true),
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
    StructField(ProductVariables.PRODUCT_NAME, StringType, true),
    StructField(ProductVariables.NUMBER_SIMPLE_PER_SKU, LongType, true),
    StructField(ProductVariables.PRICE_BAND, StringType, true),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true),
    StructField(ProductVariables.STOCK, LongType, true),
    StructField(Recommendation.NUMBER_LAST_30_DAYS_ORDERED, LongType, true),
    StructField(Recommendation.WEEKLY_AVERAGE_SALE, DoubleType, true),
    StructField(Recommendation.LAST_SOLD_DATE, TimestampType, true),
    StructField(Recommendation.DISCOUNT_STATUS, StringType, true)

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
    StructField(ProductVariables.PRODUCT_NAME, StringType, true),
    StructField(ProductVariables.NUMBER_SIMPLE_PER_SKU, LongType, true),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true),
    StructField(ProductVariables.STOCK, LongType, true),
    StructField(ProductVariables.CREATED_AT, StringType, true)
  ))

  val recommendationSku = StructType(Array(
    StructField(Recommendation.SALES_ORDER_ITEM_SKU, StringType, true),
    StructField(ProductVariables.BRICK, StringType, true),
    StructField(ProductVariables.MVP, StringType, true),
    StructField(ProductVariables.BRAND, StringType, true),
    StructField(ProductVariables.GENDER, StringType, true),
    StructField(ProductVariables.PRODUCT_NAME, StringType, true),
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
      StructField(ProductVariables.MVP, StringType, true), StructField(ProductVariables.BRAND, StringType, true), StructField(ProductVariables.SKU_SIMPLE, StringType, true), StructField(ProductVariables.GENDER, StringType, true), StructField(ProductVariables.PRODUCT_NAME, StringType, true)))), false)))

  val genRecInput = StructType(Array(
    StructField(CustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(CampaignMergedFields.REF_SKU, StringType, false),
    StructField(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, IntegerType, true),
    StructField(CampaignMergedFields.LIVE_CART_URL, StringType, true),
    StructField(CampaignMergedFields.REC_SKUS, ArrayType(StructType(Array(StructField(ProductVariables.SKU, StringType, true)))), false),
    StructField(TestConstants.TEST_CASE_FILTER, LongType, true)))

  val refSkuInput = StructType(Array(
    StructField(CustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(ProductVariables.SKU_SIMPLE, StringType, false),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true),
    StructField(ProductVariables.MVP, StringType, false),
    StructField(ProductVariables.GENDER, StringType, true),
    StructField(ProductVariables.BRICK, StringType, true),
    StructField(ProductVariables.BRAND, StringType, true),
    StructField(ProductVariables.PRODUCT_NAME, StringType, true)))

  val basicSimpleItr = StructType(Array(
    StructField(ProductVariables.SKU_SIMPLE, StringType, true),
    StructField(ProductVariables.BRICK, StringType, true),
    StructField(ProductVariables.MVP, StringType, true),
    StructField(ProductVariables.BRAND, StringType, true),
    StructField(ProductVariables.CATEGORY, StringType, true),
    StructField(ProductVariables.GENDER, StringType, true),
    StructField(ProductVariables.PRODUCT_NAME, StringType, true),
    StructField(ProductVariables.PRICE_BAND, StringType, true),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true),
    StructField(ProductVariables.STOCK, LongType, true),
    StructField(ProductVariables.COLOR, LongType, true),
    StructField(ProductVariables.ACTIVATED_AT, TimestampType, true),
    StructField(ProductVariables.CREATED_AT, StringType, true)
  ))

  val salesOrderPaybackEarn = StructType(Array(StructField(PaybackCustomerVariables.FK_SALES_ORDER, IntegerType, true)))

  val salesOrderPaybackRedeem = StructType(Array(StructField(PaybackCustomerVariables.FK_CUSTOMER, LongType, true)))

  val paybackCustomer = StructType(Array(StructField(PaybackCustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(PaybackCustomerVariables.IS_PAYBACK, BooleanType, true)))

  val groupTestOut = StructType(Array(
    StructField(CustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true),
    StructField(ProductVariables.SKU_SIMPLE, StringType, false),
    StructField(ProductVariables.MVP, StringType, false),
    StructField(ProductVariables.GENDER, StringType, true),
    StructField(ProductVariables.BRICK, StringType, true),
    StructField(ProductVariables.BRAND, StringType, true)))

  // CUSTOMER_APP_DETAILS related Schemas
  val customerAppDetails = StructType(Array(
    StructField(CustomerAppDetails.UID, StringType, true),
    StructField(CustomerVariables.DOMAIN, StringType, true),
    StructField(SalesOrderVariables.CREATED_AT, TimestampType, true),
    StructField(CustomerAppDetails.FIRST_LOGIN_TIME, TimestampType, true),
    StructField(CustomerAppDetails.LAST_LOGIN_TIME, TimestampType, true),
    StructField(CustomerAppDetails.SESSION_KEY, StringType, true),
    StructField(CustomerAppDetails.ORDER_COUNT, IntegerType, true)
  ))

  val customerSession = StructType(Array(
    StructField(CustomerVariables.ID_CUSTOMER_SESSION, LongType, true),
    StructField(CustomerAppDetails.SESSION_KEY, StringType, true),
    StructField(CustomerVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerAppDetails.LOGIN_TIME, TimestampType, true),
    StructField(SalesOrderVariables.CREATED_AT, TimestampType, true),
    StructField(SalesOrderVariables.UPDATED_AT, TimestampType, true)
  ))

  val customerFavList = StructType(Array(
    StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
    StructField("brand_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, false), StructField("price", DoubleType, false), StructField("sku", StringType, false)))), true),
    StructField("catagory_list", MapType(StringType, MapType(IntegerType, DoubleType)), true),
    StructField("brick_list", MapType(StringType, MapType(IntegerType, DoubleType)), true),
    StructField("color_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, false), StructField("price", DoubleType, false)))), true),
    StructField("last_order_created_at", TimestampType, true)
  ))
  
  val cityMapSchema = StructType(Array(
    StructField(SalesAddressVariables.CITY, StringType, true),
    StructField("brand_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true)),
    StructField("brick_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true)),
    StructField("gender_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true)),
    StructField("mvp_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true))))
}
