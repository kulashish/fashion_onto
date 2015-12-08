package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ CustomerSelection, CampaignMergedFields, CampaignCommon }
import com.jabong.dap.common.constants.variables.{ SalesOrderVariables, CustomerVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 14/8/15.
 */
object WishlistCampaignQuality extends BaseCampaignQuality with Logging {

  val campaignName = "WishlistCampaignQuality"

  def getName(): String = {
    campaignName
  }

  /**
   * Consists of all the validation components for Backward test
   * @param fullShortlistData
   * @param sampleCampaignDF
   * @return
   */
  def validateWishlistCampaign(fullShortlistData: DataFrame, sampleCampaignDF: DataFrame): Boolean = {

    if ((fullShortlistData == null) || sampleCampaignDF == null) {
      return sampleCampaignDF == null
    }

    validate(fullShortlistData, sampleCampaignDF)
  }

  /**
   * One component checking if the data in sample output is present in fullShortlistData"
   * @param fullShortlistData
   * @param sampleCampaignDF
   * @return
   */
  def validate(fullShortlistData: DataFrame, sampleCampaignDF: DataFrame): Boolean = {

    val lastDayCustomerSelected = fullShortlistData.select(
      col(CustomerVariables.FK_CUSTOMER),
      col(CustomerVariables.SKU)
    ).dropDuplicates()

    val wishlistCampaignDF = sampleCampaignDF.select(
      col(CampaignMergedFields.CUSTOMER_ID),
      col(CampaignMergedFields.REF_SKU1)
    ).dropDuplicates()

    lastDayCustomerSelected.intersect(wishlistCampaignDF).count() == wishlistCampaignDF.count()
  }

  /**
   *
   * @param date in 2015/08/01 format
   * @return
   */
  def getInputOutput(date: String = TimeUtils.YESTERDAY_FOLDER): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    val fullShortlistData = CampaignInput.loadFullShortlistData(date)
    val wishListCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.WISH_LIST)

    val todayDate = TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_MS)

    val shortlistYesterdayData = CampaignInput.loadNthDayTableData(1, fullShortlistData, SalesOrderVariables.CREATED_AT, todayDate)

    val lastDayCustomerShortlistData = wishListCustomerSelector.customerSelection(shortlistYesterdayData)

    val shortlistLast30DayData = CampaignInput.loadLastNDaysTableData(30, fullShortlistData, CustomerVariables.CREATED_AT, todayDate)
    val last30DaysCustomerShortlistData = wishListCustomerSelector.customerSelection(shortlistLast30DayData)

    val wishlistFollowupCampaignDF = CampaignInput.getCampaignData(CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN, DataSets.PUSH_CAMPAIGNS, date)
    val wishlistIODCampaignDF = CampaignInput.getCampaignData(CampaignCommon.WISHLIST_IOD_CAMPAIGN, DataSets.PUSH_CAMPAIGNS, date)
    val wishlistLowStockCampaignDF = CampaignInput.getCampaignData(CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN, DataSets.PUSH_CAMPAIGNS, date)

    return (last30DaysCustomerShortlistData, lastDayCustomerShortlistData, wishlistFollowupCampaignDF, wishlistIODCampaignDF, wishlistLowStockCampaignDF)

  }

  /**
   * Entry point
   * Backward test means, getting a sample of campaign output, then for each entries in the sample,
   * we try to find the expected data in the campaign input Dataframes
   * @param date
   * @param fraction
   * @return
   */
  def backwardTest(date: String, fraction: Double): Boolean = {

    val (last30DaysCustomerShortlistData, lastDayCustomerShortlistData, wishlistFllowupCampaignDF, wishlistIODCampaignDF, wishlistLowStockCampaignDF) = getInputOutput(date)

    val samplewishlistFllowupCampaignDF = getSample(wishlistFllowupCampaignDF, fraction)
    val samplewishlistIODCampaignDF = getSample(wishlistIODCampaignDF, fraction)
    val samplewishlistLowStockCampaignDF = getSample(wishlistLowStockCampaignDF, fraction)

    val statusFolloup = validateWishlistCampaign(lastDayCustomerShortlistData, samplewishlistFllowupCampaignDF)
    logger.info("Status of Wishlist Fllowup: " + statusFolloup)

    val statusIOD = validateWishlistCampaign(last30DaysCustomerShortlistData, samplewishlistIODCampaignDF)
    logger.info("Status of Wishlist IOD: " + statusIOD)

    val statusLowStock = validateWishlistCampaign(last30DaysCustomerShortlistData, samplewishlistLowStockCampaignDF)
    logger.info("Status of Wishlist Low Stock: " + statusLowStock)

    return (statusFolloup && statusIOD && statusLowStock)

  }

}

