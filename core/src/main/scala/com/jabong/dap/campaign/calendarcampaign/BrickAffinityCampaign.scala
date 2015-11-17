package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CampaignMergedFields, CustomerSelection, CampaignCommon }
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.{ Row, DataFrame }

/**
 * Created by raghu on 29/10/15.
 */
class BrickAffinityCampaign {

  val BRICK1 = "BRICK1"
  val BRICK2 = "BRICK2"
  def runCampaign(customerSurfAffinity: DataFrame, last7thDaySalesOrderData: DataFrame, last7thDaySalesOrderItemData: DataFrame, brickMvpRecommendations: DataFrame, yesterdayItrData: DataFrame) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelection = customerSelector.customerSelection(last7thDaySalesOrderData, last7thDaySalesOrderItemData)

    //filter sku based on daily filter
    val filteredSku = Daily.skuFilter(dfCustomerSelection, yesterdayItrData)

    //call generate ref sku for maximum sku price [email, ref-sku]
    //    val refSkus = CampaignUtils.generateReferenceSkus(dfCustomerSelection, CampaignCommon.CALENDAR_REF_SKUS)

    //join Customer Favorite data [email, Brick1, Brick2, Brand1, Brand2]
    val customerFavBrick = getCustomerFavBrick(customerSurfAffinity)

    //join Customer Favorite data [email, ref-sku, Brick1, Brick2]
    val joinedToFavBrick = filteredSku.join(customerFavBrick, filteredSku(CustomerVariables.EMAIL) === customerFavBrick(CustomerVariables.EMAIL), SQL.INNER)
      .select(
        filteredSku("*"),
        customerFavBrick(BRICK1),
        customerFavBrick(BRICK2)
      )

    //FIXME: Generate 8 sku from Brick1 from seller recommendation [email, ref-sku, Brick1 -> [sku1 to sku8]]
    //FIXME: Generate 8 sku from Brick2 from seller recommendation [email, ref-sku, Brick2 -> [sku1 to sku8]]
    //FIXME: join Brick1 to Brick2 [email, ref-sku, Brick1 -> [sku1 to sku8], Brick2 -> [sku1 to sku8]]

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.BRICK_AFFINITY_CAMPAIGN, joinedToFavBrick, false, brickMvpRecommendations)

  }

  def getCustomerFavBrick(customerSurfAffinity: DataFrame): DataFrame = {
    val topBricks = customerSurfAffinity.select(CustomerVariables.EMAIL, "brick_list"
    ).rdd.map(r => (r(0).toString, r(1).asInstanceOf[Map[String, Row]].toSeq.sortBy(r => (r._2(r._2.fieldIndex("count")).asInstanceOf[Int], r._2(r._2.fieldIndex("sum_price")).asInstanceOf[Double])) (Ordering.Tuple2(Ordering.Int.reverse, Ordering.Double.reverse)).map(_._1)))

    val topTwoBricks = topBricks.map{ case (key, value) => ({ val arrayLength = value.length; if (arrayLength >= 2) (key, value(0), value(1)) else if (arrayLength == 1) (key, value(0), null) else (key, null, null) }) }

    val sqlContext = Spark.getSqlContext()
    import sqlContext.implicits._

    topTwoBricks.toDF(CustomerVariables.EMAIL, BRICK1, BRICK2)

  }
}
