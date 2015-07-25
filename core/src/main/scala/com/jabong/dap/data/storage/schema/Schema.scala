package com.jabong.dap.data.storage.schema

import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables._
import org.apache.spark.sql.types._

/**
 * Created by raghu on 22/6/15.
 */
object Schema {

  val customer = StructType(Array(
    StructField(CustomerVariables.ID_CUSTOMER, IntegerType, true),
    StructField(CustomerVariables.EMAIL, StringType, true),
    StructField(CustomerVariables.INCREMENT_ID, StringType, true),
    StructField(CustomerVariables.PREFIX, StringType, true),
    StructField(CustomerVariables.FIRST_NAME, StringType, true),
    StructField(CustomerVariables.MIDDLE_NAME, StringType, true),
    StructField(CustomerVariables.LAST_NAME, StringType, true),
    StructField(CustomerVariables.BIRTHDAY, DateType, true),
    StructField(CustomerVariables.GENDER, StringType, true),
    StructField(CustomerVariables.PHONE, StringType, true),
    StructField(CustomerVariables.PASSWORD, StringType, true),
    StructField(CustomerVariables.RESTORE_PASSWORD_KEY, StringType, true),
    StructField(CustomerVariables.IS_CONFIRMED, BooleanType, true),
    StructField(CustomerVariables.EMAIL_VERIFIED_AT, TimestampType, true),
    StructField(CustomerVariables.IS_MOBILE_VERIFIED, BooleanType, true),
    StructField(CustomerVariables.MOBILE_VERIFIED_AT, TimestampType, true),
    StructField(CustomerVariables.CONFIRMATION_KEY, StringType, true),
    StructField(CustomerVariables.PAYMENT_REFERENCE_KEY, StringType, true),
    StructField(CustomerVariables.CREATED_AT, TimestampType, true),
    StructField(CustomerVariables.UPDATED_AT, TimestampType, true),
    StructField(CustomerVariables.STORE_CREDITS_AVAILABLE, DecimalType(10, 2), true),
    StructField(CustomerVariables.STORE_CREDITS_VALIDITY, DateType, true),
    StructField(CustomerVariables.GIFTCARD_CREDITS_AVAILABLE, DecimalType(10, 2), true),
    StructField(CustomerVariables.GIFTCARD_CREDITS_VALIDITY, DateType, true),
    StructField(CustomerVariables.SESSION_COOKIE, StringType, true),
    StructField(CustomerVariables.LOGGED_IN, TimestampType, true),
    StructField(CustomerVariables.REWARD_TYPE, StringType, true),
    StructField(CustomerVariables.DOMAIN, StringType, true),
    StructField(CustomerVariables.USER_DEVICE_TYPE, StringType, true),
    StructField(CustomerVariables.API_TOKEN, StringType, true),
    StructField(CustomerVariables.APP_LOGGED_IN, TimestampType, true),
    StructField(CustomerVariables.ID_CUSTOMER_ADDITIONAL_INFO, IntegerType, true),
    StructField(CustomerVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerVariables.SOURCE, StringType, true),
    StructField(CustomerVariables.CITY, StringType, true),
    StructField(CustomerVariables.RELATIONSHIP_STATUS, StringType, true),
    StructField(CustomerVariables.ANNIVERSARY_DATE, DateType, true),
    StructField(CustomerVariables.FACEBOOK_UID, StringType, true),
    StructField(CustomerVariables.FACEBOOK_PIC_URL, StringType, true),
    StructField(CustomerVariables.FACEBOOK_LINKED_DATE, TimestampType, true),
    StructField(CustomerVariables.IS_VIA_FBCONNECT, BooleanType, true),
    StructField(CustomerVariables.CALCULATED_GENDER, StringType, true),
    StructField(CustomerVariables.GOOGLE_UID, StringType, true),
    StructField(CustomerVariables.GOOGLE_PIC_URL, StringType, true),
    StructField(CustomerVariables.GOOGLE_LINKED_DATE, TimestampType, true),
    StructField(CustomerVariables.IS_VIA_GOOGLECONNECT, BooleanType, true),
    StructField(CustomerVariables.GOOGLE_REFRESH_TOKEN, StringType, true),
    StructField(CustomerVariables.FB_ACCESS_TOKEN, StringType, true),
    StructField(CustomerVariables.APP_VERSION, StringType, true),
    StructField(CustomerVariables.FK_CORPORATE_CUSTOMER, IntegerType, true),
    StructField(CustomerVariables.FK_REFERRAL_CODE, IntegerType, true),
    StructField(CustomerVariables.SMS_OPT, BooleanType, true)
  ))

  val nls = StructType(Array(
    StructField(NewsletterVariables.ID_NEWSLETTER_SUBSCRIPTION, IntegerType, true),
    StructField(NewsletterVariables.FK_CUSTOMER, IntegerType, true),
    StructField(NewsletterVariables.EMAIL, StringType, true),
    StructField(NewsletterVariables.UNSUBSCRIBE_KEY, StringType, true),
    StructField(NewsletterVariables.IP, StringType, true),
    StructField(NewsletterVariables.CREATED_AT, TimestampType, true),
    StructField(NewsletterVariables.STATUS, StringType, true),
    StructField(NewsletterVariables.GENDER, StringType, true),
    StructField(NewsletterVariables.UPDATED_AT, TimestampType, true),
    StructField(NewsletterVariables.FK_NEWSLETTER_CATEGORY, IntegerType, true),
    StructField(NewsletterVariables.NEWSLETTER_PREFERENCES, StringType, true),
    StructField(NewsletterVariables.FK_AFFILIATE_PARTNER, IntegerType, true),
    StructField(NewsletterVariables.SRC_SUB, StringType, true),
    StructField(NewsletterVariables.SRC_UNSUB, StringType, true),
    StructField(NewsletterVariables.FREQUENCY, StringType, true)
  ))

  val salesOrder = StructType(Array(
    StructField(SalesOrderVariables.ID_SALES_ORDER, IntegerType, true),
    StructField(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_BILLING, IntegerType, true),
    StructField(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING, IntegerType, true),
    StructField(SalesOrderVariables.FK_CUSTOMER, IntegerType, true),
    StructField(SalesOrderVariables.CUSTOMER_FIRST_NAME, StringType, true),
    StructField(SalesOrderVariables.CUSTOMER_LAST_NAME, StringType, true),
    StructField(SalesOrderVariables.CUSTOMER_EMAIL, StringType, true),
    StructField(SalesOrderVariables.ORDER_NR, StringType, true),
    StructField(SalesOrderVariables.CUSTOMER_SESSION_ID, StringType, true),
    StructField(SalesOrderVariables.STORE_ID, IntegerType, true),
    StructField(SalesOrderVariables.GRAND_TOTAL, DecimalType(10, 2), true),
    StructField(SalesOrderVariables.TAX_AMOUNT, DecimalType(10, 2), true),
    StructField(SalesOrderVariables.SHIPPING_AMOUNT, DecimalType(10, 2), true),
    StructField(SalesOrderVariables.SHIPPING_METHOD, StringType, true),
    StructField(SalesOrderVariables.COUPON_CODE, StringType, true),
    StructField(SalesOrderVariables.PAYMENT_METHOD, StringType, true),
    StructField(SalesOrderVariables.CREATED_AT, TimestampType, true),
    StructField(SalesOrderVariables.UPDATED_AT, TimestampType, true),
    StructField(SalesOrderVariables.FK_SHIPPING_CARRIER, IntegerType, true),
    StructField(SalesOrderVariables.TRACKING_URL, StringType, true),
    StructField(SalesOrderVariables.OTRS_TICKET, StringType, true),
    StructField(SalesOrderVariables.FK_SALES_ORDER_PROCESS, IntegerType, true),
    StructField(SalesOrderVariables.SHIPPING_DISCOUNT_AMOUNT, DecimalType(10, 0), true),
    StructField(SalesOrderVariables.IP, StringType, true),
    StructField(SalesOrderVariables.INVOICE_FILE, StringType, true),
    StructField(SalesOrderVariables.INVOICE_NR, StringType, true),
    StructField(SalesOrderVariables.IS_RECURRING, BooleanType, true),
    StructField(SalesOrderVariables.CCAVENUE_ORDER_NUMBER, StringType, true),
    StructField(SalesOrderVariables.COD_CHARGE, DecimalType(10, 2), true),
    StructField(SalesOrderVariables.RETRIAL, BooleanType, true),
    StructField(SalesOrderVariables.ID_SALES_ORDER_ADDITIONAL_INFO, IntegerType, true),
    StructField(SalesOrderVariables.FK_SALES_ORDER, IntegerType, true),
    StructField(SalesOrderVariables.FK_AFFILIATE_PARTNER, IntegerType, true),
    StructField(SalesOrderVariables.FK_SHIPPING_PARTNER_AGENT, IntegerType, true),
    StructField(SalesOrderVariables.DOMAIN, StringType, true),
    StructField(SalesOrderVariables.USER_DEVICE_TYPE, StringType, true),
    StructField(SalesOrderVariables.SHIPMENT_DELAY_DAYS, IntegerType, true),
    StructField(SalesOrderVariables.MOBILE_VERIFICATION, StringType, true),
    StructField(SalesOrderVariables.ADDRESS_MISMATCH, IntegerType, true),
    StructField(SalesOrderVariables.EARN_METHOD, StringType, true),
    StructField(SalesOrderVariables.PARENT_ORDER_ID, IntegerType, true),
    StructField(SalesOrderVariables.UTM_CAMPAIGN, StringType, true),
    StructField(SalesOrderVariables.REWARD_POINTS, DecimalType(10, 2), true),
    StructField(SalesOrderVariables.APP_VERSION, StringType, true),
    StructField(SalesOrderVariables.FK_CORPORATE_CUSTOMER, IntegerType, true),
    StructField(SalesOrderVariables.CORPORATE_CURRENCY_VALUE, DecimalType(10, 2), true),
    StructField(SalesOrderVariables.CORPORATE_TRANSACTION_ID, StringType, true),
    StructField(SalesOrderVariables.DEVICE_ID, StringType, true)
  ))

  val csh = StructType(Array(
    StructField(CustomerStoreVariables.ID_CUSTOMER_STORECREDITS_HISTORY, IntegerType, true),
    StructField(CustomerStoreVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerStoreVariables.CREATED_AT, TimestampType, true),
    StructField(CustomerStoreVariables.OPERATION_TYPE, StringType, true),
    StructField(CustomerStoreVariables.FK_OPERATION, IntegerType, true),
    StructField(CustomerStoreVariables.STORE_CREDITS_VALUE, DecimalType(10, 2), true),
    StructField(CustomerStoreVariables.CREDIT_TYPE, StringType, true),
    StructField(CustomerStoreVariables.TRANSACTION_TYPE, IntegerType, true),
    StructField(CustomerStoreVariables.TRANSACTION_VALUE, DecimalType(10, 2), true),
    StructField(CustomerStoreVariables.NOTE, StringType, true),
    StructField(CustomerStoreVariables.FK_ACL_USER, IntegerType, true),
    StructField(CustomerStoreVariables.UPDATED_AT, TimestampType, true),
    StructField(CustomerStoreVariables.BALANCE, DecimalType(10, 2), true),
    StructField(CustomerStoreVariables.EXPIRY_DATE, DateType, true)
  ))

  val customerSegments = StructType(Array(
    StructField(CustomerSegmentsVariables.ID_CUSTOMER_SEGMENTS, IntegerType, true),
    StructField(CustomerSegmentsVariables.SEGMENT, IntegerType, true),
    StructField(CustomerSegmentsVariables.FREQUENCY, IntegerType, true),
    StructField(CustomerSegmentsVariables.RECENCY, IntegerType, true),
    StructField(CustomerSegmentsVariables.MVP_SCORE, IntegerType, true),
    StructField(CustomerSegmentsVariables.DISCOUNT_SCORE, IntegerType, true),
    StructField(CustomerSegmentsVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerSegmentsVariables.CREATED_AT, TimestampType, true),
    StructField(CustomerSegmentsVariables.UPDATED_AT, TimestampType, true)
  ))

  val salesOrderPaybackEarn = StructType(Array(StructField(PaybackCustomerVariables.FK_SALES_ORDER, IntegerType, true)))
  val salesOrderPaybackRedeem = StructType(Array(StructField(PaybackCustomerVariables.FK_CUSTOMER, IntegerType, true)))
  val paybackCustomer = StructType(Array(StructField(PaybackCustomerVariables.FK_CUSTOMER, IntegerType, true),
    StructField(PaybackCustomerVariables.IS_PAYBACK, BooleanType, true)))

  val customerProductShortlist = StructType(Array(
    StructField(CustomerProductShortlistVariables.ID_CUSTOMER_PRODUCT_SHORTLIST, IntegerType, true),
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerProductShortlistVariables.USER_SHORTLIST_KEY, StringType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(CustomerProductShortlistVariables.SKU, StringType, true),
    StructField(CustomerProductShortlistVariables.EXTRA_DATA, StringType, true),
    StructField(CustomerProductShortlistVariables.STOCK_WHEN_REMOVED, IntegerType, true),
    StructField(CustomerProductShortlistVariables.CUSTOMER_SOURCE, StringType, true),
    StructField(CustomerProductShortlistVariables.CREATED_AT, TimestampType, true),
    StructField(CustomerProductShortlistVariables.REMOVED_AT, TimestampType, true),
    StructField(CustomerProductShortlistVariables.DOMAIN, StringType, true),
    StructField(CustomerProductShortlistVariables.USER_DEVICE_TYPE, StringType, true)
  ))

  val resultCustomerProductShortlist = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(CustomerProductShortlistVariables.SKU, StringType, true),
    StructField(CustomerProductShortlistVariables.DOMAIN, StringType, true),
    StructField(CustomerProductShortlistVariables.USER_DEVICE_TYPE, StringType, true),
    StructField(CustomerProductShortlistVariables.CREATED_AT, TimestampType, true),
    StructField(CustomerProductShortlistVariables.SKU_SIMPLE, StringType, true),
    StructField(CustomerProductShortlistVariables.PRICE, DecimalType(10, 2), true)
  ))

  val itr = StructType(Array(
    StructField(ItrVariables.SKU, StringType, true),
    StructField(ItrVariables.SKU_SIMPLE, StringType, true),
    StructField(ItrVariables.BRAND, StringType, true),
    StructField(ItrVariables.BRICK, StringType, true),
    StructField(ItrVariables.MVP, IntegerType, true),
    StructField(ItrVariables.GENDER, StringType, true),
    StructField(ItrVariables.SPECIAL_PRICE, DecimalType(10, 2), true),
    StructField(ItrVariables.AVERAGE_PRICE, DecimalType(10, 2), true),
    StructField(ItrVariables.WEEKLY_AVERAGE_SALE, DecimalType(10, 2), true),
    StructField(ItrVariables.AVERAGE_STOCK, IntegerType, true),
    StructField(ItrVariables.STOCK, IntegerType, true),
    StructField(ItrVariables.CREATED_AT, TimestampType, true)
  ))

  val resultGetJoin = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(CustomerProductShortlistVariables.SKU_SIMPLE, StringType, true),
    StructField(ItrVariables.SPECIAL_PRICE, DecimalType(10, 2), true)
  ))

  val resultSkuSimpleFilter = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(CustomerProductShortlistVariables.SKU_SIMPLE, StringType, true),
    StructField(CustomerProductShortlistVariables.SPECIAL_PRICE, DecimalType(10, 2), true)
  ))

  val resultSkuFilter = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(CustomerProductShortlistVariables.SKU, StringType, true),
    StructField(CustomerProductShortlistVariables.AVERAGE_PRICE, DecimalType(10, 2), true)
  ))

  val resultFullSkuFilter = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(ProductVariables.SKU_SIMPLE, StringType, true),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true)
  ))

  val campaignOutput =  StructType(Array(
    StructField(CampaignMergedFields.FK_CUSTOMER, IntegerType, true),
    StructField(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, IntegerType, true),
    StructField(CampaignMergedFields.REF_SKU1, StringType, true),
    StructField(CampaignMergedFields.REF_SKU2,StringType, true)
  ))


  val salesOrderItem = StructType(Array(
    StructField(SalesOrderItemVariables.ID_SALES_ORDER_ITEM, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_SALES_ORDER, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_SALES_MERCHANT_ORDER, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_SHIPMENT, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_MERCHANT, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_MARKETPLACE_MERCHANT, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_SALES_ORDER_ADDRESS_WAREHOUSE, IntegerType, true),
    StructField(SalesOrderItemVariables.UNIT_PRICE, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.TAX_AMOUNT, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.PAID_PRICE, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.STORE_CREDITS_VALUE, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.GIFTCARD_CREDITS_VALUE, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.COUPON_MONEY_VALUE, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.COUPON_PERCENT, IntegerType, true),
    StructField(SalesOrderItemVariables.COUPON_REFUNDABLE, IntegerType, true),
    StructField(SalesOrderItemVariables.COUPON_CATEGORY, IntegerType, true),
    StructField(SalesOrderItemVariables.NAME, StringType, true),
    StructField(SalesOrderItemVariables.SKU, StringType, true),
    StructField(SalesOrderItemVariables.WEIGHT, DecimalType(12, 4), true),
    StructField(SalesOrderItemVariables.CREATED_AT, TimestampType, true),
    StructField(SalesOrderItemVariables.UPDATED_AT, TimestampType, true),
    StructField(SalesOrderItemVariables.ADDITIONAL_TEXT, StringType, true),
    StructField(SalesOrderItemVariables.LAST_STATUS_CHANGE, TimestampType, true),
    StructField(SalesOrderItemVariables.AMOUNT_PAID, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.REFUNDED_MONEY, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.REFUNDED_VOUCHER, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.TAX_PERCENT, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.ORIGINAL_UNIT_PRICE, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.CART_RULE_DISCOUNT, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.CART_RULE_DISPLAY_NAMES, StringType, true),
    StructField(SalesOrderItemVariables.FK_CATALOG_SHIPMENT_TYPE, IntegerType, true),
    StructField(SalesOrderItemVariables.IS_RESERVED, IntegerType, true),
    StructField(SalesOrderItemVariables.DELIVERY_TIME, StringType, true),
    StructField(SalesOrderItemVariables.DISPATCH_TIME, IntegerType, true),
    StructField(SalesOrderItemVariables.BUNDLE_DISCOUNT, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.FK_SKU_BUNDLE, IntegerType, true),
    StructField(SalesOrderItemVariables.IS_FREEBIE, IntegerType, true),
    StructField(SalesOrderItemVariables.SHIPPING_CHARGE, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.ID_SALES_ORDER_ITEM_ADDITIONAL_INFO, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_SALES_ORDER_ITEM, IntegerType, true),
    StructField(SalesOrderItemVariables.IS_CANCELABLE, IntegerType, true),
    StructField(SalesOrderItemVariables.IS_RETURNABLE, IntegerType, true),
    StructField(SalesOrderItemVariables.IS_COD, IntegerType, true),
    StructField(SalesOrderItemVariables.NOT_BUYABLE, IntegerType, true),
    StructField(SalesOrderItemVariables.PROCESSING_TIME, StringType, true),
    StructField(SalesOrderItemVariables.EXPECTED_DISPATCH_DATE, DateType, true),
    StructField(SalesOrderItemVariables.EXPECTED_DELIVERY_DATE, DateType, true),
    StructField(SalesOrderItemVariables.IS_DATE_CHANGED, IntegerType, true),
    StructField(SalesOrderItemVariables.PACK_ID, IntegerType, true),
    StructField(SalesOrderItemVariables.BUNDLE_PACK_IDENTIFIER, StringType, true),
    StructField(SalesOrderItemVariables.PACK_QTY, IntegerType, true),
    StructField(SalesOrderItemVariables.MARGIN, DecimalType(6, 2), true),
    StructField(SalesOrderItemVariables.SHIPPING_LIABILITY, StringType, true),
    StructField(SalesOrderItemVariables.DISPATCH_LOCATION, StringType, true),
    StructField(SalesOrderItemVariables.HOLIDAY_COUNT, IntegerType, true),
    StructField(SalesOrderItemVariables.IS_GIFT_WRAPPED, IntegerType, true),
    StructField(SalesOrderItemVariables.MODE_OF_DISCOUNT_AMOUNT, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.MODE_OF_PAYMENT_AMOUNT, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.DISCOUNT_CAP, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.VAT_AMOUNT, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.IS_VAT_CHARGED, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_CATALOG_ATTRIBUTE_OPTION_GLOBAL_ORDER_TYPE, IntegerType, true),
    StructField(SalesOrderItemVariables.PRE_ORDER_ITEM_DISPATCH_DATE, DateType, true),
    StructField(SalesOrderItemVariables.PRE_ORDER_ITEM_CAMPAIGN_NAME, StringType, true),
    StructField(SalesOrderItemVariables.PAYBACK_CREDITS_VALUE, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.IS_PAYBACK_REFUNDED, IntegerType, true),
    StructField(SalesOrderItemVariables.IS_CUSTOMIZED, IntegerType, true),
    StructField(SalesOrderItemVariables.CUSTOMIZATION_COST, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.PAYBACK_EARN_VALUE, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.REWARD_POINTS, DecimalType(10, 2), true),
    StructField(SalesOrderItemVariables.PROCESSED_BITMAP, IntegerType, true),
    StructField(SalesOrderItemVariables.EXPECTED_SHIPPING_PARTNER, StringType, true),
    StructField(SalesOrderItemVariables.IS_MULTIPLE_SHIPMENTS, IntegerType, true),
    StructField(SalesOrderItemVariables.CORPORATE_CURRENCY_VALUE, DecimalType(10, 2), true)
  ))


}
