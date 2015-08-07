package com.jabong.dap.model.product.itr

import com.jabong.dap.common.Spark
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

/**
 * Data sources for ITR
 */
object Model {

  var config, simple, salesOrderItem, catalogStock, productImage, categoryMapping, itemMaster: DataFrame = null
  var supplier, brand, category: Broadcast[DataFrame] = null

  def getItrInputs(date: String) {

    config = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_config", DataSets.FULL_MERGE_MODE, date)

    simple = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_simple", DataSets.FULL_MERGE_MODE, date)

    supplier = Spark.getContext().broadcast(DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_supplier", DataSets.FULL_FETCH_MODE, date))

    brand = Spark.getContext().broadcast(DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_brand", DataSets.FULL_FETCH_MODE, date))

    salesOrderItem = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "sales_order_item", DataSets.FULL_MERGE_MODE, date)

    catalogStock = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_stock", DataSets.FULL_FETCH_MODE, date)

    productImage = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_product_image", DataSets.FULL_FETCH_MODE, date)

    category = Spark.getContext().broadcast(DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_category", DataSets.FULL_FETCH_MODE, date))

    categoryMapping = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_config_has_catalog_category", DataSets.FULL_FETCH_MODE, date)

    itemMaster = DataReader.getDataFrame(DataSets.INPUT_PATH, "erp", "item_master_complete_dump", DataSets.FULL_MERGE_MODE, date)

  }
}

