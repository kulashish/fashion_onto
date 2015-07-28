package com.jabong.dap.common.constants.campaign

/**
 * Created by rahul for common campaign constants  on 9/7/15.
 */
object CampaignCommon {
  val SKU_SELECTOR = "SkuSelector"
  val CUSTOMER_SELECTOR = "CustomerSelector"
  val RECOMMENDER = "Recommender"

  val REF_SKUS = "refSkus"
  val PUSH_REF_SKUS = 1
  val NUMBER_REF_SKUS = 2

  val BASE_PATH = "/data/output/campaigns/"

  val CANCEL_RETARGET_CAMPAIGN = "cancel_retarget"
  val RETURN_RETARGET_CAMPAIGN = "return_retarget"
  val INVALID_FOLLOWUP_CAMPAIGN = "invalid_followup"
  val INVALID_LOWSTOCK_CAMPAIGN = "invalid_lowstock"
  val ACART_FOLLOWUP_CAMPAIGN = "acart_followup"
  val ACART_DAILY_CAMPAIGN = "acart_daily"
  val ACART_LOWSTOCK_CAMPAIGN = "acart_lowstock"
  val ACART_IOD_CAMPAIGN = "acart_iod"
  val WISHLIST_IOD_CAMPAIGN = "wishlist_iod"
  val SURF1_CAMPAIGN = "surf1"
  val SURF2_CAMPAIGN = "surf2"
  val SURF3_CAMPAIGN = "surf3"
  val SURF6_CAMPAIGN = "surf6"
  val MERGED_CAMPAIGN = "merged"
  val WISHLIST_FOLLOWUP_CAMPAIGN = "wishlist_followup"
  val WISHLIST_LOWSTOCK_CAMPAIGN = "wishlist_lowstock"




  val LOW_STOCK_VALUE = 10
  val FOLLOW_UP_STOCK_VALUE = 10
  val DATE_FORMAT = "yyyy/MM/dd"

  val INVALID_CAMPAIGN = "invalidCampaign"
  val WISHLIST_CAMPAIGN = "wishlistCampaign"

  val VERY_LOW_PRIORITY = 10000

  val PRIORITY = "priority"

  val campaignMailTypeMap = collection.immutable.HashMap(
    "cancel_retarget" -> 46,
    "return_retarget" -> 47,
    "acart_daily" -> 42,
    "wishlist_followup" -> 53,
    "acart_iod" -> 45,
    "acart_iod" -> 54,
    "acart_lowstock" -> 44,
    "wishlist_lowstock" -> 55,
    "acart_followup" -> 43,
    "surf2" -> 57,
    "surf6" -> 71,
    "invalid_followup" -> 43,
    "invalid_lowstock" -> 49,
    "surf1" -> 56,
    "surf3" -> 58
  )

}
