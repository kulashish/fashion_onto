package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.customerselection.YesterdaySessionDistinct
import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
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

  var surf: Surf = _

  override def beforeAll() {

    super.beforeAll()

    surf = new Surf()

    //    JsonUtils.writeToJson("/home/raghu/bigData/parquetFiles/", "customer_surf_data")
    dfCustomerPageVisit = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.SURF, DataSets.CUSTOMER_PAGE_VISIT, Schema.customerPageVisitSkuLevel)
    dfItrData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.SURF, DataSets.ITR_30_DAY_DATA, Schema.itr)
    dfCustomer = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.SURF, DataSets.CUSTOMER, Schema.customer)
    dfSalesOrder = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.SURF, DataSets.SALES_ORDER, Schema.salesOrder)
    dfSalesOrderItem = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.SURF, DataSets.SALES_ORDER_ITEM, Schema.salesOrderItem)

  }

  "skuFilter(a,b,c,d,e): All Data Frame " should "null" in {

    val result = surf.skuFilter(null, null, null, null, null)

    assert(result == null)

  }

  "skuFilter(a,b,c,d,e): count " should "3" in {

    val result = surf.skuFilter(dfCustomerPageVisit, dfItrData, dfCustomer, dfSalesOrder, dfSalesOrderItem)

    assert(result.count() == 3)
  }

}