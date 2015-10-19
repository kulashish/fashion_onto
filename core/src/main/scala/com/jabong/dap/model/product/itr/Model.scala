package com.jabong.dap.model.product.itr

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.config.ConfigConstants
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

    config = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_CONFIG, DataSets.FULL_MERGE_MODE, date)

    simple = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_SIMPLE, DataSets.FULL_MERGE_MODE, date)

    supplier = Spark.getContext().broadcast(DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_SUPPLIER, DataSets.FULL_FETCH_MODE, date))

    brand = Spark.getContext().broadcast(DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_BRAND, DataSets.FULL_FETCH_MODE, date))

    salesOrderItem = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.FULL_MERGE_MODE, date)

    catalogStock = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_STOCK, DataSets.FULL_FETCH_MODE, date)

    productImage = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_PRODUCT_IMAGE, DataSets.FULL_FETCH_MODE, date)

    category = Spark.getContext().broadcast(DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_CATEGORY, DataSets.FULL_FETCH_MODE, date))

    categoryMapping = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_CONFIG_HAS_CATALOG_CATEGORY, DataSets.FULL_FETCH_MODE, date)

    itemMaster = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.ERP, DataSets.ITEM_MASTER_COMPLETE_DUMP, DataSets.FULL_MERGE_MODE, date)

  }
}

