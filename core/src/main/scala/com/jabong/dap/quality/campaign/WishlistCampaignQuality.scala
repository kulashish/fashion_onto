package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.CustomerProductShortlistVariables
import com.jabong.dap.common.time.TimeUtils
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 14/8/15.
 */
object WishlistCampaignQuality extends BaseCampaignQuality with Logging {

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
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.SKU)
    ).dropDuplicates()

    val wishlistCampaignDF = sampleCampaignDF.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.SKU)
    ).dropDuplicates()

    lastDayCustomerSelected.intersect(wishlistCampaignDF).count() == wishlistCampaignDF.count()
  }

  /**
   *
   * @param date in 2015/08/01 format
   * @return
   */
  def getInputOutput(date: String = TimeUtils.YESTERDAY_FOLDER): (DataFrame, DataFrame, DataFrame, DataFrame) = {

    val fullShortlistData = CampaignInput.loadFullShortlistData()

    val wishlistFllowupCampaignDF = CampaignInput.getCampaignData(CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN, date)
    val wishlistIODCampaignDF = CampaignInput.getCampaignData(CampaignCommon.WISHLIST_IOD_CAMPAIGN, date)
    val wishlistLowStockCampaignDF = CampaignInput.getCampaignData(CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN, date)

    return (fullShortlistData, wishlistFllowupCampaignDF, wishlistIODCampaignDF, wishlistLowStockCampaignDF)

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

    val (fullShortlistData, wishlistFllowupCampaignDF, wishlistIODCampaignDF, wishlistLowStockCampaignDF) = getInputOutput(date)

    val samplewishlistFllowupCampaignDF = getSample(wishlistFllowupCampaignDF, fraction)
    val samplewishlistIODCampaignDF = getSample(wishlistIODCampaignDF, fraction)
    val samplewishlistLowStockCampaignDF = getSample(wishlistLowStockCampaignDF, fraction)

    val statusFolloup = validateWishlistCampaign(fullShortlistData, samplewishlistFllowupCampaignDF)
    logger.info("Status of Wishlist Fllowup: " + statusFolloup)

    val statusIOD = validateWishlistCampaign(fullShortlistData, samplewishlistIODCampaignDF)
    logger.info("Status of Wishlist IOD: " + statusIOD)

    val statusLowStock = validateWishlistCampaign(fullShortlistData, samplewishlistLowStockCampaignDF)
    logger.info("Status of Wishlist Low Stock: " + statusLowStock)

    return (statusFolloup && statusIOD && statusLowStock)

  }

}

