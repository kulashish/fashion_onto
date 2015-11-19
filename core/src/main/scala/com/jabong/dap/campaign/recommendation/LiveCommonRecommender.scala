package com.jabong.dap.campaign.recommendation

import com.jabong.dap.campaign.recommendation.generator.RecommendationUtils
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{ Recommendation, CampaignMergedFields }
import com.jabong.dap.common.constants.variables.{SalesAddressVariables, CustomerVariables, ProductVariables}
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.schema.Schema
import grizzled.slf4j.Logging
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by rahul aneja on 21/8/15.
 */
class LiveCommonRecommender extends Recommender with Logging {
  /**
   * Place holder function which will get recommended skus
   * @param refSkus
   * @param recommendations
   * @return
   */
  override def generateRecommendation(refSkus: DataFrame, recommendations: DataFrame, recType: String = Recommendation.BRICK_MVP_SUB_TYPE, numRecSkus: Int = 8): DataFrame = {
    require(refSkus != null, "refSkus cannot be null")
    require(recommendations != null, "recommendations cannot be null")
    require(Array(Recommendation.BRICK_MVP_SUB_TYPE, Recommendation.BRAND_MVP_SUB_TYPE, Recommendation.BRICK_PRICE_BAND_SUB_TYPE, Recommendation.MVP_DISCOUNT_SUB_TYPE, Recommendation.MVP_COLOR_SUB_TYPE) contains recType, "recommendation type is invalid")
    var refSkusUpdatedSchema: DataFrame = refSkus
    if (!SchemaUtils.isSchemaEqual(refSkus.schema, Schema.expectedFinalReferenceSku)) {
      refSkusUpdatedSchema = SchemaUtils.addColumns(refSkus, Schema.expectedFinalReferenceSku)
    }

    CampaignUtils.debug(recommendations, "after recommendations")

    val refSkuExploded = refSkusUpdatedSchema.select(
      refSkusUpdatedSchema(CustomerVariables.EMAIL),
      refSkusUpdatedSchema(CampaignMergedFields.REF_SKU1),
      refSkusUpdatedSchema(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
      refSkusUpdatedSchema(CampaignMergedFields.LIVE_CART_URL),
      explode(refSkus(CampaignMergedFields.REF_SKUS)) as "ref_sku_fields")

    CampaignUtils.debug(refSkuExploded, "after refSkuExploded")

    //FIXME: To check if there is any ref sku in recommended sku
    //FIXME: add column as rec skus instead of passing entire data to genRecSkus function
    val completeRefSku = refSkuExploded.select(
      refSkuExploded(CustomerVariables.EMAIL),
      refSkuExploded(CampaignMergedFields.REF_SKU1),
      refSkuExploded(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
      refSkuExploded(CampaignMergedFields.LIVE_CART_URL),
      refSkuExploded("ref_sku_fields.brick") as ProductVariables.BRICK,
      refSkuExploded("ref_sku_fields.mvp") as ProductVariables.MVP,
      refSkuExploded("ref_sku_fields.gender") as ProductVariables.GENDER,
      refSkuExploded("ref_sku_fields.brand") as ProductVariables.BRAND,
      refSkuExploded("ref_sku_fields.productName") as ProductVariables.PRODUCT_NAME,
      refSkuExploded("ref_sku_fields.priceBand") as ProductVariables.PRICE_BAND,
      refSkuExploded("ref_sku_fields.color") as ProductVariables.COLOR,
      refSkuExploded("ref_sku_fields.city") as SalesAddressVariables.CITY,
      refSkuExploded("ref_sku_fields.skuSimple") as CampaignMergedFields.REF_SKU)

    CampaignUtils.debug(completeRefSku, "after completeRefSku")

    val recommendationJoined = joinToRecommendation(completeRefSku, recommendations, recType)

    CampaignUtils.debug(recommendationJoined, "after recommendationJoined")

    val recommendationSelected = recommendationJoined.select(
      completeRefSku(CustomerVariables.EMAIL),
      // recommendedSkus(completeRefSku(CampaignMergedFields.REF_SKU), recommendations(CampaignMergedFields.RECOMMENDATIONS)) as CampaignMergedFields.REC_SKUS,
      recommendations(CampaignMergedFields.RECOMMENDATIONS + "." + ProductVariables.SKU) as CampaignMergedFields.REC_SKUS,
      Udf.skuFromSimpleSku(completeRefSku(CampaignMergedFields.REF_SKU)) as CampaignMergedFields.REF_SKU,
      completeRefSku(ProductVariables.BRAND) as CampaignMergedFields.LIVE_BRAND,
      completeRefSku(ProductVariables.BRICK) as CampaignMergedFields.LIVE_BRICK,
      completeRefSku(ProductVariables.PRODUCT_NAME) as CampaignMergedFields.LIVE_PROD_NAME,
      completeRefSku(ProductVariables.COLOR) as CampaignMergedFields.CALENDAR_COLOR,
      completeRefSku(SalesAddressVariables.CITY) as CampaignMergedFields.CALENDAR_CITY,
      completeRefSku(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
      completeRefSku(CampaignMergedFields.LIVE_CART_URL))

    CampaignUtils.debug(recommendationSelected, "after recommendationSelected")

    val recommendationGrouped = recommendationSelected.map(row => ((row(0)), (row))).groupByKey().map({ case (key, value) => (key.asInstanceOf[String], getRecSkus(value, numRecSkus)) })
      .map({ case (key, value) => (key, value._1, value._2, value._3, value._4) })

    val sqlContext = Spark.getSqlContext()
    import sqlContext.implicits._
    val campaignDataWithRecommendations = recommendationGrouped.toDF(CustomerVariables.EMAIL, CampaignMergedFields.REF_SKUS,
      CampaignMergedFields.REC_SKUS, CampaignMergedFields.CAMPAIGN_MAIL_TYPE, CampaignMergedFields.LIVE_CART_URL)

    CampaignUtils.debug(campaignDataWithRecommendations, "after campaignDataWithRecommendations")

    return campaignDataWithRecommendations
  }

  def joinToRecommendation(completeRefSku: DataFrame, recommendations: DataFrame, recType: String): DataFrame = {

    require(completeRefSku != null, "completeRefSku data frame should not null")
    require(recommendations != null, "recommendations data frame should not null")
    require(RecommendationUtils.recommendationType.contains(recType), "recommendations data frame should not null")

    val recommendationJoined = recType match {

      case Recommendation.BRAND_MVP_SUB_TYPE => {
        completeRefSku.join(recommendations, completeRefSku(ProductVariables.BRAND) === recommendations(ProductVariables.BRAND)
          && completeRefSku(ProductVariables.MVP) === recommendations(ProductVariables.MVP)
          && completeRefSku(ProductVariables.GENDER) === recommendations(ProductVariables.GENDER))

      }

      case Recommendation.MVP_COLOR_SUB_TYPE => {
        completeRefSku.join(recommendations, completeRefSku(ProductVariables.COLOR) === recommendations(ProductVariables.COLOR)
          && completeRefSku(ProductVariables.MVP) === recommendations(ProductVariables.MVP)
          && completeRefSku(ProductVariables.GENDER) === recommendations(ProductVariables.GENDER))

      }

      case Recommendation.BRAND_MVP_CITY_SUB_TYPE => {
        completeRefSku.join(recommendations, completeRefSku(ProductVariables.BRAND) === recommendations(ProductVariables.BRAND)
          && completeRefSku(ProductVariables.MVP) === recommendations(ProductVariables.MVP)
          && completeRefSku(SalesAddressVariables.CITY) === recommendations(SalesAddressVariables.CITY))
      }

      case Recommendation.BRICK_PRICE_BAND_SUB_TYPE => {

        val dfNextPriceBand = completeRefSku.select(
          col(CustomerVariables.EMAIL),
          col(CampaignMergedFields.REF_SKU1),
          col(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
          col(CampaignMergedFields.LIVE_CART_URL),
          col(ProductVariables.BRICK),
          col(ProductVariables.MVP),
          col(ProductVariables.GENDER),
          col(ProductVariables.BRAND),
          col(ProductVariables.PRODUCT_NAME),
          Udf.nextPriceBand(col(ProductVariables.PRICE_BAND)) as ProductVariables.PRICE_BAND,
          col(CampaignMergedFields.REF_SKU)
        )

        CampaignUtils.debug(dfNextPriceBand, "inside join dfNextPriceBand")
        CampaignUtils.debug(completeRefSku, "inside join completeRefSku")
        CampaignUtils.debug(recommendations, "inside join recommendations")

        completeRefSku.join(recommendations, completeRefSku(ProductVariables.BRICK) === recommendations(ProductVariables.BRICK)
          && Udf.nextPriceBand(completeRefSku(ProductVariables.PRICE_BAND)) === recommendations(ProductVariables.PRICE_BAND)
          && completeRefSku(ProductVariables.GENDER) === recommendations(ProductVariables.GENDER))
      }
      case Recommendation.MVP_DISCOUNT_SUB_TYPE => {
        val completeRefSkuWithDiscountStatus = completeRefSku.withColumn(Recommendation.DISCOUNT_STATUS, lit("true"))

        completeRefSkuWithDiscountStatus.join(recommendations, completeRefSkuWithDiscountStatus(Recommendation.DISCOUNT_STATUS) === recommendations(Recommendation.DISCOUNT_STATUS)
          && completeRefSkuWithDiscountStatus(ProductVariables.MVP) === recommendations(ProductVariables.MVP)
          && completeRefSkuWithDiscountStatus(ProductVariables.GENDER) === recommendations(ProductVariables.GENDER))
      }
      case _ => {
        completeRefSku.join(recommendations, completeRefSku(ProductVariables.BRICK) === recommendations(ProductVariables.BRICK)
          && completeRefSku(ProductVariables.MVP) === recommendations(ProductVariables.MVP)
          && completeRefSku(ProductVariables.GENDER) === recommendations(ProductVariables.GENDER))
      }
    }

    return recommendationJoined
  }

  //  val recommendedSkus = udf((refSkus: String, recommendations: List[((Row))]) => getRecommendedSkus(refSkus: String, recommendations: List[(Row)]))
  //  /**
  //   *
  //   * @param refSku
  //   * @param recommendation
  //   * @return
  //   */
  //  def getRecommendedSkus(refSku: String, recommendation: List[(Row)]): List[(Row)] = {
  //    require(refSku != null, "refSkus cannot be null")
  //    require(recommendation != null, "recommendation cannot be null")
  //    println("refSkus:-"+refSku)
  //    val outputSkus = recommendation.filterNot(x => x(1) == refSku).take(Recommendation.NUM_REC_SKU_REF_SKU).map(x => x(1).toString())
  //    return recommendation
  //  }
  /**
   * return recommended skus based on number of reference sku (if ref sku is 1 then all 8 rec sku and if its 2 ,then 4 rec sku each)
   * @param iterable
   * @return
   */
  def getRecSkus(iterable: Iterable[Row], numRecSkus: Int): (mutable.MutableList[(String, String, String, String)], mutable.MutableList[String], Int, String) = {
    require(iterable != null, "iterable cannot be null")
    require(iterable.size != 0, "iterable cannot be of size zero")

    val topRow = iterable.head
    val recommendedSkus: mutable.MutableList[String] = mutable.MutableList()
    val referenceSkus: mutable.MutableList[(String, String, String, String)] = mutable.MutableList()
    val recommendationIndex = topRow.fieldIndex(CampaignMergedFields.REC_SKUS)
    val campaignMailTypeIndex = topRow.fieldIndex(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
    val acartUrlIndex = topRow.fieldIndex(CampaignMergedFields.LIVE_CART_URL)
    val mailType = topRow(campaignMailTypeIndex).asInstanceOf[Int]
    val acartUrl = CampaignUtils.checkNullString(topRow(acartUrlIndex))
    val refSkuIndex = topRow.fieldIndex(CampaignMergedFields.REF_SKU)
    val liveBrandIndex = topRow.fieldIndex(CampaignMergedFields.LIVE_BRAND)
    val liveBrickIndex = topRow.fieldIndex(CampaignMergedFields.LIVE_BRICK)
    val liveProdNameIndex = topRow.fieldIndex(CampaignMergedFields.LIVE_PROD_NAME)
    val calendarColorIndex = topRow.fieldIndex(CampaignMergedFields.CALENDAR_COLOR)
    val calendarCityIndex = topRow.fieldIndex(CampaignMergedFields.CALENDAR_CITY)

    val numberRefSku = iterable.size
    val skuPerIteration = if (numberRefSku == 1) numRecSkus else 4
    for (row <- iterable) {
      var i = 1;
      val recommendations = row(recommendationIndex).asInstanceOf[scala.collection.mutable.ArrayBuffer[String]].
        foreach(value => if (!recommendedSkus.contains(value) && i <= skuPerIteration) { recommendedSkus += value; i = i + 1; })

      referenceSkus += ((row(refSkuIndex).toString, CampaignUtils.checkNullString(row(liveBrandIndex)), CampaignUtils.checkNullString(row(liveBrickIndex)),
        CampaignUtils.checkNullString(row(liveProdNameIndex),CampaignUtils.checkNullString(row(calendarColorIndex),CampaignUtils.checkNullString(row(calendarCityIndex))))))

    }
    return (referenceSkus, recommendedSkus, mailType, acartUrl)
  }

}
