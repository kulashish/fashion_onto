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
  val CALENDAR_REF_SKUS = 1
  val CALENDAR_REC_SKUS = 16

  val CALENDAR_MIN_RECS = 4
  val BASE_PATH = "/data/output/campaigns/"

  //Live campaigns
  val CANCEL_RETARGET_CAMPAIGN = "cancel_retarget"
  val RETURN_RETARGET_CAMPAIGN = "return_retarget"
  val INVALID_FOLLOWUP_CAMPAIGN = "invalid_followup"
  val INVALID_LOWSTOCK_CAMPAIGN = "invalid_lowstock"
  val INVALID_IOD_CAMPAIGN = "invalid_iod"
  val ACART_FOLLOWUP_CAMPAIGN = "acart_followup"
  val ACART_DAILY_CAMPAIGN = "acart_daily"
  val ACART_HOURLY_CAMPAIGN = "acart_hourly"
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

  val FOLLOW_UP_CAMPAIGNS = "follow_up_campaigns"

  //calendar campaigns
  val BRAND_IN_CITY_CAMPAIGN = "brand_in_city"
  val PRICEPOINT_CAMPAIGN = "pricepoint"
  val BRICK_AFFINITY_CAMPAIGN = "brick_affinity"
  val NON_BEAUTY_FRAG_CAMPAIGN = "non_beauty_frag"
  val BEAUTY_CAMPAIGN = "beauty_campaign"
  val HOTTEST_X = "hottest_x"
  val CLEARANCE_CAMPAIGN = "clearance"
  val LOVE_BRAND_CAMPAIGN = "love_brand"

  val LOW_STOCK_VALUE = 10
  val FOLLOW_UP_STOCK_VALUE = 10
  val ACART_HOURLY_STOCK_VALUE = 2
  val LAST_FIVE_PURCHASES = 5

  val INVALID_CAMPAIGN = "invalidCampaign"
  val WISHLIST_CAMPAIGN = "wishlistCampaign"

  val VERY_LOW_PRIORITY = 10000

  val PRIORITY = "priority"

  val ACART_BASE_URL = "www.jabong.com/cart/addmulti?skus="
  //add following source name with other params in AppConfig.config.credentials
  val J_DARE_SOURCE = "jDaReSource"

  val MOBILE_PUSH_CAMPAIGN_QUALITY = "mobile_push_campaign_quality"
  val EMAIL_CAMPAIGN_QUALITY = "email_campaign_quality"

  val MIPR_CAMPAIGN = "mipr"
  val NEW_ARRIVALS_BRAND = "new_arrivals_brand"
  val SHORTLIST_REMINDER = "shortlist_reminder"
  val COUNT_NEW_ARRIVALS = 4

  //follow up campaigns
  val CANCEL_RETARGET_MAIL_TYPE = 46
  val CANCEL_RETARGET_FOLLOW_UP_MAIL_TYPE = 76
  val SURF6_MAIL_TYPE = 71
  val SURF6_FOLLOW_UP_MAIL_TYPE = 75
  val RETURN_RETARGET_MAIL_TYPE = 47
  val RETURN_RETARGET_FOLLOW_UP_MAIL_TYPE = 77
  val SURF1_MAIL_TYPE = 56
  val SURF1_FOLLOW_UP_MAIL_TYPE = 72
  val SURF2_MAIL_TYPE = 57
  val SURF2_FOLLOW_UP_MAIL_TYPE = 73
  val SURF3_MAIL_TYPE = 58
  val SURF3_FOLLOW_UP_MAIL_TYPE = 74

  val campaignMailTypeMap = collection.immutable.HashMap(
    "cancel_retarget" -> 46,
    "return_retarget" -> 47,
    "acart_daily" -> 42,
    "wishlist_followup" -> 53,
    "acart_iod" -> 45,
    "wishlist_iod" -> 54,
    "acart_lowstock" -> 44,
    "wishlist_lowstock" -> 55,
    "acart_followup" -> 43,
    "surf2" -> 57,
    "surf6" -> 71,
    "invalid_followup" -> 48,
    "invalid_lowstock" -> 49,
    "surf1" -> 56,
    "surf3" -> 58,
    "mipr" -> 67,
    "new_arrivals_brand" -> 68,
    "shortlist_reminder" -> 53,
    "invalid_iod" -> 100,
    "acart_hourly" -> 41,
    CampaignCommon.PRICEPOINT_CAMPAIGN -> 24,
    //FIXME: put correct mail type
    CampaignCommon.BEAUTY_CAMPAIGN -> 400,
    //FIXME: put correct mail type
    CampaignCommon.NON_BEAUTY_FRAG_CAMPAIGN -> 500,
    CampaignCommon.BRAND_IN_CITY_CAMPAIGN -> 24,
    CampaignCommon.BRICK_AFFINITY_CAMPAIGN -> 14,
    CampaignCommon.CLEARANCE_CAMPAIGN -> 20,
    CampaignCommon.LOVE_BRAND_CAMPAIGN -> 13,
    CampaignCommon.HOTTEST_X -> 11
  )

  val campaignRecommendationMap = collection.immutable.HashMap(
    CampaignCommon.PRICEPOINT_CAMPAIGN -> Recommendation.BRICK_PRICE_BAND_SUB_TYPE,
    CampaignCommon.CLEARANCE_CAMPAIGN -> Recommendation.MVP_DISCOUNT_SUB_TYPE,
    CampaignCommon.LOVE_BRAND_CAMPAIGN -> Recommendation.BRAND_MVP_SUB_TYPE
  )
}
