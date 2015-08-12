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

    config = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_CONFIG, DataSets.FULL_MERGE_MODE)

    simple = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_SIMPLE, DataSets.FULL_MERGE_MODE)

    supplier = Spark.getContext().broadcast(DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_SUPPLIER, DataSets.FULL_FETCH_MODE))

    brand = Spark.getContext().broadcast(DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_BRAND, DataSets.FULL_FETCH_MODE))

    salesOrderItem = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.FULL_MERGE_MODE)

    catalogStock = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_STOCK, DataSets.FULL_FETCH_MODE)

    productImage = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_PRODUCT_IMAGE, DataSets.FULL_FETCH_MODE)

    category = Spark.getContext().broadcast(DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_CATEGORY, DataSets.FULL_FETCH_MODE))

    categoryMapping = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_CONFIG_HAS_CATALOG_CATEGORY, DataSets.FULL_FETCH_MODE)

    itemMaster: DataFrame = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.ERP, DataSets.ITEM_MASTER_COMPLETE_DUMP, DataSets.FULL_MERGE_MODE)

  }
}

