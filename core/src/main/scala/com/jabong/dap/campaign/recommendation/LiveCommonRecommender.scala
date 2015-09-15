package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{ Recommendation, CampaignMergedFields }
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables }
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
  override def generateRecommendation(refSkus: DataFrame, recommendations: DataFrame): DataFrame = {
    require(refSkus != null, "refSkus cannot be null")
    require(recommendations != null, "recommendations cannot be null")

    refSkus.printSchema()

    val refSkuExploded = refSkus.select(
      refSkus(CustomerVariables.FK_CUSTOMER),
      refSkus(CampaignMergedFields.REF_SKU1),
      refSkus(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
      explode(refSkus(CampaignMergedFields.REF_SKUS)) as "ref_sku_fields")

    refSkuExploded.printSchema()
    val completeRefSku = refSkuExploded.select(
      refSkuExploded(CustomerVariables.FK_CUSTOMER),
      refSkuExploded(CampaignMergedFields.REF_SKU1),
      refSkuExploded(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
      refSkuExploded("ref_sku_fields.brick") as ProductVariables.BRICK,
      refSkuExploded("ref_sku_fields.mvp") as ProductVariables.MVP,
      refSkuExploded("ref_sku_fields.gender") as ProductVariables.GENDER,
      refSkuExploded("ref_sku_fields.skuSimple") as CampaignMergedFields.REF_SKU)
    println("OUTTESTDATA:-"+completeRefSku.show(10));

    val recommendationJoined = completeRefSku.join(recommendations, completeRefSku(ProductVariables.BRICK) === recommendations(ProductVariables.BRICK)
      && completeRefSku(ProductVariables.MVP) === completeRefSku(ProductVariables.MVP)
      && completeRefSku(ProductVariables.GENDER) === recommendations(ProductVariables.GENDER))
      .select(
        completeRefSku(CustomerVariables.FK_CUSTOMER),
        recommendedSkus(completeRefSku(CampaignMergedFields.REF_SKU), recommendations(CampaignMergedFields.RECOMMENDATIONS)) as CampaignMergedFields.REC_SKUS,
        completeRefSku(CampaignMergedFields.REF_SKU),
        completeRefSku(CampaignMergedFields.CAMPAIGN_MAIL_TYPE))
    println("TESTDATA"+recommendationJoined.show(10))
    val recommendationGrouped = recommendationJoined.map(row => ((row(0)), (row))).groupByKey().map({ case (key, value) => (key.asInstanceOf[String], getRecSkus(value)) })
      .map({ case (key, value) => (key, value._1, value._2, value._3) })
    println("DATATEST"+recommendationGrouped.take(5))
    val sqlContext = Spark.getSqlContext()
    import sqlContext.implicits._
    val campaignDataWithRecommendations = recommendationGrouped.toDF(CustomerVariables.FK_CUSTOMER, CampaignMergedFields.REF_SKU,
      CampaignMergedFields.REC_SKUS, CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
    println("DATATEST:-----"+campaignDataWithRecommendations.take(5))

    return campaignDataWithRecommendations
  }

  val recommendedSkus = udf((refSkus: String, recommendations: List[Row]) => getRecommendedSkus(refSkus: String, recommendations: List[Row]))
  /**
   *
   * @param refSku
   * @param recommendation
   * @return
   */
  def getRecommendedSkus(refSku: Long, recommendation: List[Row]): List[(String)] = {
    require(refSku != null, "refSkus cannot be null")
    require(recommendation != null, "recommendation cannot be null")
    val y: List[String] = null
    println("refSkus:-"+refSku)
    val outputSkus = recommendation.filterNot(x => x.getString(1) == refSku).take(Recommendation.NUM_REC_SKU_REF_SKU).map(x => x(1).toString())
    return outputSkus
  }

  def getRecSkus(iterable: Iterable[Row]): (mutable.MutableList[String], mutable.MutableList[String], Int) = {
    require(iterable != null, "iterable cannot be null")
    require(iterable.size != 0, "iterable cannot be of size zero")

    val topRow = iterable.head
    val recommendedSkus: mutable.MutableList[String] = mutable.MutableList()
    val referenceSkus: mutable.MutableList[String] = mutable.MutableList()
    val recommendationIndex = topRow.fieldIndex(CampaignMergedFields.REC_SKUS)
    val campaignMailTypeIndex = topRow.fieldIndex(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
    val mailType = topRow(campaignMailTypeIndex).asInstanceOf[Int]
    val refSkuIndex = topRow.fieldIndex(CampaignMergedFields.REF_SKU)
    val numberRefSku = iterable.size
    val skuPerIteration = if (numberRefSku == 1) 8 else 4
    for (row <- iterable) {
      var i = 1;
      val recommendations = row(recommendationIndex).asInstanceOf[List[String]].foreach(value => if (!recommendedSkus.contains(value) && i <= skuPerIteration) { recommendedSkus += value; i = i + 1; })
      referenceSkus += row(refSkuIndex).toString
    }
    return (referenceSkus, recommendedSkus, mailType)
  }

}
