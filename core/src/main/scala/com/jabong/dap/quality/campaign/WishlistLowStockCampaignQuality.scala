
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
object WishlistLowStockCampaignQuality extends BaseCampaignQuality {

  /**
   * Consists of all the validation components for Backward test
   * @param fullShortlistData
   * @param sampleWishlistLowStockCampaignDF
   * @return
   */
  def validate(fullShortlistData: DataFrame, sampleWishlistLowStockCampaignDF: DataFrame): Boolean = {

    if ((fullShortlistData == null) || sampleWishlistLowStockCampaignDF == null) {
      return sampleWishlistLowStockCampaignDF == null
    }

    validateWishlistLowStockCampaign(fullShortlistData, sampleWishlistLowStockCampaignDF)
  }

  /**
   * One component checking if the data in sample output is present in fullShortlistData"
   * @param fullShortlistData
   * @param sampleWishlistLowStockCampaignDF
   * @return
   */
  def validateWishlistLowStockCampaign(fullShortlistData: DataFrame, sampleWishlistLowStockCampaignDF: DataFrame): Boolean = {

    val lastDayCustomerSelected = fullShortlistData.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.SKU)
    ).dropDuplicates()

    val wishlistLowStockCampaignDF = sampleWishlistLowStockCampaignDF.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.SKU)
    ).dropDuplicates()

    lastDayCustomerSelected.intersect(wishlistLowStockCampaignDF).count() == wishlistLowStockCampaignDF.count()
  }

  /**
   *
   * @param date in 2015/08/01 format
   * @return
   */
  def getInputOutput(date: String = TimeUtils.YESTERDAY_FOLDER): (DataFrame, DataFrame) = {

    val fullShortlistData = CampaignInput.loadFullShortlistData()

    val wishlistLowStockCampaignDF = CampaignInput.getCampaignData(CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN, date)

    return (fullShortlistData, wishlistLowStockCampaignDF)

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

    val (fullShortlistData, wishlistLowStockCampaignDF) = getInputOutput(date)

    val samplewishlistLowStockCampaignDF = getSample(wishlistLowStockCampaignDF, fraction)

    validate(fullShortlistData, samplewishlistLowStockCampaignDF)
  }

}

