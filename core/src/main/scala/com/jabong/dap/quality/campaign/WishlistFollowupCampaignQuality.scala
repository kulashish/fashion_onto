package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.CustomerProductShortlistVariables
import com.jabong.dap.common.time.TimeUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 14/8/15.
 */
object WishlistFollowupCampaignQuality extends BaseCampaignQuality {

  /**
   * Consists of all the validation components for Backward test
   * @param fullShortlistData
   * @param sampleWishlistFllowupCampaignDF
   * @return
   */
  def validate(fullShortlistData: DataFrame, sampleWishlistFllowupCampaignDF: DataFrame): Boolean = {

    if ((fullShortlistData == null) || sampleWishlistFllowupCampaignDF == null) {
      return sampleWishlistFllowupCampaignDF == null
    }

    validateWishlistFllowupCampaign(fullShortlistData, sampleWishlistFllowupCampaignDF)
  }

  /**
   * One component checking if the data in sample output is present in fullShortlistData"
   * @param fullShortlistData
   * @param sampleWishlistFllowupCampaignDF
   * @return
   */
  def validateWishlistFllowupCampaign(fullShortlistData: DataFrame, sampleWishlistFllowupCampaignDF: DataFrame): Boolean = {

    val lastDayCustomerSelected = fullShortlistData.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.SKU)
    ).dropDuplicates()

    val wishlistFllowupCampaignDF = sampleWishlistFllowupCampaignDF.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.SKU)
    ).dropDuplicates()

    lastDayCustomerSelected.intersect(wishlistFllowupCampaignDF).count() == wishlistFllowupCampaignDF.count()
  }

  /**
   *
   * @param date in 2015/08/01 format
   * @return
   */
  def getInputOutput(date: String = TimeUtils.YESTERDAY_FOLDER): (DataFrame, DataFrame) = {

    val fullShortlistData = CampaignInput.loadFullShortlistData()

    val wishlistFllowupCampaignDF = CampaignInput.getCampaignData(CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN, date)

    return (fullShortlistData, wishlistFllowupCampaignDF)

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

    val (fullShortlistData, wishlistFllowupCampaignDF) = getInputOutput(date)

    val samplewishlistFllowupCampaignDF = getSample(wishlistFllowupCampaignDF, fraction)

    validate(fullShortlistData, samplewishlistFllowupCampaignDF)
  }

}
