package com.jabong.dap.campaign.recommendation

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{ Recommendation, CampaignMergedFields }
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables }
import com.jabong.dap.common.schema.SchemaUtils
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
  override def generateRecommendation(refSkus: DataFrame, recommendations: DataFrame, recType: String = Recommendation.BRICK_MVP_SUB_TYPE): DataFrame = {
    require(refSkus != null, "refSkus cannot be null")
    require(recommendations != null, "recommendations cannot be null")
    require(Array(Recommendation.BRICK_MVP_SUB_TYPE, Recommendation.BRAND_MVP_SUB_TYPE) contains recType, "recommendation type is invalid")
    var refSkusUpdatedSchema: DataFrame = refSkus
    if (!SchemaUtils.isSchemaEqual(refSkus.schema, Schema.expectedFinalReferenceSku)) {
      refSkusUpdatedSchema = SchemaUtils.changeSchema(refSkus, Schema.expectedFinalReferenceSku)
    }

    val refSkuExploded = refSkusUpdatedSchema.select(
      refSkusUpdatedSchema(CustomerVariables.FK_CUSTOMER),
      refSkusUpdatedSchema(CampaignMergedFields.REF_SKU1),
      refSkusUpdatedSchema(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
      refSkusUpdatedSchema(CampaignMergedFields.LIVE_CART_URL),
      explode(refSkus(CampaignMergedFields.REF_SKUS)) as "ref_sku_fields")

    //FIXME: To check if there is any ref sku in recommended sku
    //FIXME: add column as rec skus instead of passing entire data to genRecSkus function
    val completeRefSku = refSkuExploded.select(
      refSkuExploded(CustomerVariables.FK_CUSTOMER),
      refSkuExploded(CampaignMergedFields.REF_SKU1),
      refSkuExploded(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
      refSkuExploded(CampaignMergedFields.LIVE_CART_URL),
      refSkuExploded("ref_sku_fields.brick") as ProductVariables.BRICK,
      refSkuExploded("ref_sku_fields.mvp") as ProductVariables.MVP,
      refSkuExploded("ref_sku_fields.gender") as ProductVariables.GENDER,
      refSkuExploded("ref_sku_fields.brand") as ProductVariables.BRAND,
      refSkuExploded("ref_sku_fields.skuSimple") as CampaignMergedFields.REF_SKU)

    var recommendationJoined: DataFrame = null

    if (recType.equalsIgnoreCase(Recommendation.BRICK_MVP_SUB_TYPE)) {
      recommendationJoined = completeRefSku.join(recommendations, completeRefSku(ProductVariables.BRICK) === recommendations(ProductVariables.BRICK)
        && completeRefSku(ProductVariables.MVP) === recommendations(ProductVariables.MVP)
        && completeRefSku(ProductVariables.GENDER) === recommendations(ProductVariables.GENDER))
    } else {
      recommendationJoined = completeRefSku.join(recommendations, completeRefSku(ProductVariables.BRAND) === recommendations(ProductVariables.BRAND)
        && completeRefSku(ProductVariables.MVP) === recommendations(ProductVariables.MVP)
        && completeRefSku(ProductVariables.GENDER) === recommendations(ProductVariables.GENDER))
    }

    val recommendationSelected = recommendationJoined.select(
      completeRefSku(CustomerVariables.FK_CUSTOMER),
      // recommendedSkus(completeRefSku(CampaignMergedFields.REF_SKU), recommendations(CampaignMergedFields.RECOMMENDATIONS)) as CampaignMergedFields.REC_SKUS,
      recommendations(CampaignMergedFields.RECOMMENDATIONS + "." + ProductVariables.SKU) as CampaignMergedFields.REC_SKUS,
      completeRefSku(CampaignMergedFields.REF_SKU),
      completeRefSku(ProductVariables.BRAND) as CampaignMergedFields.LIVE_BRAND,
      completeRefSku(ProductVariables.BRICK) as CampaignMergedFields.LIVE_BRICK,
      completeRefSku(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
      completeRefSku(CampaignMergedFields.LIVE_CART_URL))

    val recommendationGrouped = recommendationSelected.map(row => ((row(0)), (row))).groupByKey().map({ case (key, value) => (key.asInstanceOf[Long], getRecSkus(value)) })
      .map({ case (key, value) => (key, value._1, value._2, value._3, value._4) })

    val sqlContext = Spark.getSqlContext()
    import sqlContext.implicits._
    val campaignDataWithRecommendations = recommendationGrouped.toDF(CustomerVariables.FK_CUSTOMER, CampaignMergedFields.REF_SKUS,
      CampaignMergedFields.REC_SKUS, CampaignMergedFields.CAMPAIGN_MAIL_TYPE, CampaignMergedFields.LIVE_CART_URL)

    return campaignDataWithRecommendations
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
  def getRecSkus(iterable: Iterable[Row]): (mutable.MutableList[(String,String,String,String)], mutable.MutableList[String], Int, String) = {
    require(iterable != null, "iterable cannot be null")
    require(iterable.size != 0, "iterable cannot be of size zero")

    val topRow = iterable.head
    val recommendedSkus: mutable.MutableList[String] = mutable.MutableList()
    val referenceSkus: mutable.MutableList[(String,String,String,String)] = mutable.MutableList()
    val recommendationIndex = topRow.fieldIndex(CampaignMergedFields.REC_SKUS)
    val campaignMailTypeIndex = topRow.fieldIndex(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
    val acartUrlIndex = topRow.fieldIndex(CampaignMergedFields.LIVE_CART_URL)
    val mailType = topRow(campaignMailTypeIndex).asInstanceOf[Int]
    val acartUrl = CampaignUtils.checkNullString(topRow(acartUrlIndex))
    val refSkuIndex = topRow.fieldIndex(CampaignMergedFields.REF_SKU)
    val liveBrandIndex = topRow.fieldIndex(CampaignMergedFields.LIVE_BRAND)
    val liveBrickIndex = topRow.fieldIndex(CampaignMergedFields.LIVE_BRICK)
    val numberRefSku = iterable.size
    val skuPerIteration = if (numberRefSku == 1) 8 else 4
    for (row <- iterable) {
      var i = 1;
      val recommendations = row(recommendationIndex).asInstanceOf[scala.collection.mutable.ArrayBuffer[String]].
        foreach(value => if (!recommendedSkus.contains(value) && i <= skuPerIteration) { recommendedSkus += value; i = i + 1; })

      referenceSkus += ((row(refSkuIndex).toString ,CampaignUtils.checkNullString(row(liveBrandIndex))
        ,CampaignUtils.checkNullString(row(liveBrickIndex)),""))

    }
    return (referenceSkus, recommendedSkus, mailType, acartUrl)
  }

}
