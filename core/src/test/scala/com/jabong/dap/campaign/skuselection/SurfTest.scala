package com.jabong.dap.campaign.skuselection

import java.io.File

import com.jabong.dap.common.constants.campaign.SkuSelection
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, TestConstants }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 24/7/15.
 */
class SurfTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerPageVisit: DataFrame = _
  @transient var dfItrData: DataFrame = _
  @transient var dfCustomer: DataFrame = _
  @transient var dfSalesOrder: DataFrame = _
  @transient var dfSalesOrderItem: DataFrame = _

  override def beforeAll() {

    super.beforeAll()

    //    JsonUtils.writeToJson("/home/raghu/bigData/parquetFiles/", "customer_surf_data")
    dfCustomerPageVisit = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.SURF, TestConstants.CUSTOMER_PAGE_VISIT, Schema.customerPageVisitSkuLevel)
    dfItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.SURF, TestConstants.ITR_30_DAY_DATA, Schema.itr)
    dfCustomer = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.SURF, DataSets.CUSTOMER, Schema.customer)
    dfSalesOrder = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.SURF, DataSets.SALES_ORDER, Schema.salesOrder)
    dfSalesOrderItem = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.SURF, DataSets.SALES_ORDER_ITEM, Schema.salesOrderItem)

  }

  /* FIXME
  "skuFilter(a,b,c,d,e): All Data Frame " should "null" in {

    val result = Surf.skuFilter(null, null, null, null, null)

    assert(result == null)

  }
  */

  /* FIXME
  "skuFilter(a,b,c,d,e): count " should "3" in {

    val result = Surf.skuFilter(dfCustomerPageVisit, dfItrData, dfCustomer, dfSalesOrder, dfSalesOrderItem)

    assert(result.count() == 3)
  }
  */

}
