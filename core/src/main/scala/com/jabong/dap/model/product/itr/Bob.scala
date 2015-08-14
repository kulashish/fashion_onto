package com.jabong.dap.model.product.itr

import com.jabong.dap.common.constants.SQL
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.jabong.dap.model.product.itr.variables.ITR

object Bob {

  /**
   * Prepare data frame of bob related columns
   * Note: All column names are following camel casing
   * pattern
   *
   * @return DataFrame
   */
  def getBobColumns(): DataFrame = {
    val simpleDF = Model.simple.select(
      "id_catalog_simple",
      "special_price",
      "special_to_date",
      "special_from_date",
      "sku",
      "fk_catalog_config",
      "barcode_ean"
    ).withColumnRenamed("id_catalog_simple", ITR.ID_CATALOG_SIMPLE).
      withColumnRenamed("special_price", ITR.SPECIAL_PRICE).
      withColumnRenamed("special_to_date", ITR.SPECIAL_TO_DATE).
      withColumnRenamed("special_from_date", ITR.SPECIAL_FROM_DATE).
      withColumnRenamed("sku", ITR.SIMPLE_SKU).
      withColumnRenamed("barcode_ean", ITR.BARCODE_EAN)

    val visibilityDataFrame = visibilityDF()
    val vDF = visibilityDataFrame.select(
      ITR.ID_CATALOG_SIMPLE,
      ITR.VISIBILITY
    ).
      withColumnRenamed(
        ITR.ID_CATALOG_SIMPLE,
        "vIdCatalogSimple"
      )

    val visibilityDaFr = simpleDF.join(
      vDF,
      vDF.col("vIdCatalogSimple") === simpleDF.col(ITR.ID_CATALOG_SIMPLE),
      SQL.LEFT_OUTER
    )

    val stockDataFrame = stockDF()
    val sDF = stockDataFrame.select(
      ITR.ID_CATALOG_SIMPLE,
      ITR.QUANTITY
    ).
      withColumnRenamed(
        ITR.ID_CATALOG_SIMPLE,
        "stockIdCatalogSimple"
      )

    val quantityDF = visibilityDaFr.join(
      sDF,
      sDF.col("stockIdCatalogSimple") === simpleDF.col(ITR.ID_CATALOG_SIMPLE),
      SQL.LEFT_OUTER
    )

    val config = Model.config.select(
      "id_catalog_config",
      "name",
      "special_margin",
      "margin",
      "activated_at",
      "sku",
      "fk_catalog_supplier",
      "fk_catalog_brand"
    ).
      withColumnRenamed("id_catalog_config", ITR.ID_CATALOG_CONFIG).
      withColumnRenamed("name", ITR.PRODUCT_NAME).
      withColumnRenamed("special_margin", ITR.SPECIAL_MARGIN).
      withColumnRenamed("activated_at", ITR.ACTIVATED_AT).
      withColumnRenamed("sku", ITR.CONFIG_SKU)

    val configDF = quantityDF.join(
      config,
      config(ITR.ID_CATALOG_CONFIG) === simpleDF("fk_catalog_config")
    )

    val supplierDF = configDF.join(
      Model.supplier.value.select("status", "id_catalog_supplier").withColumnRenamed("status", "supplierStatus"),
      Model.config("fk_catalog_supplier") === Model.supplier.value("id_catalog_supplier")
    )

    val productUrl = udf(url)

    val brandDF = supplierDF.join(
      Model.brand.value.select("url_key", "id_catalog_brand", "name").
        withColumnRenamed("url_key", "brandUrlKey").
        withColumnRenamed("name", ITR.BRAND_NAME),
      configDF("fk_catalog_brand") === Model.brand.value("id_catalog_brand")
    ).withColumn(
        ITR.PRODUCT_URL,
        productUrl(col(ITR.ID_CATALOG_CONFIG), col("brandUrlKey"), col(ITR.PRODUCT_NAME))
      )

    brandDF.
      select(
        ITR.ID_CATALOG_SIMPLE,
        ITR.SPECIAL_PRICE,
        ITR.SPECIAL_TO_DATE,
        ITR.SPECIAL_FROM_DATE,
        ITR.SIMPLE_SKU,
        ITR.BARCODE_EAN,
        ITR.VISIBILITY,
        ITR.QUANTITY,
        ITR.ID_CATALOG_CONFIG,
        ITR.PRODUCT_NAME,
        ITR.ACTIVATED_AT,
        ITR.CONFIG_SKU,
        ITR.SUPPLIER_STATUS,
        ITR.PRODUCT_URL,
        ITR.SPECIAL_MARGIN,
        ITR.MARGIN,
        ITR.BRAND_NAME
      )
  }

  /**
   * Data frame of stock columns of data frame sku & quantity
   *
   * @return DataFrame
   */
  def stockDF(): DataFrame = {
    val reservedDF = simpleReservedDF()
    val stockUDF = udf(stock)

    Model.simple.select(
      "id_catalog_simple",
      "sku"
    ).join(
        Model.catalogStock.select("fk_catalog_simple", "quantity"),
        Model.simple.col("id_catalog_simple") === Model.catalogStock.col("fk_catalog_simple"),
        SQL.LEFT_OUTER
      ).join(
          reservedDF,
          Model.simple.col("sku") === reservedDF.col("simpleSku")
        ).withColumn(
            "quantity",
            stockUDF(col("quantity"), col("reservedCount"))
          ).select(
              "id_catalog_simple",
              "quantity"
            ).withColumnRenamed(
                "id_catalog_simple",
                "idCatalogSimple"
              )
  }

  /**
   * Calculate stock against simple sku
   *
   * @return Long
   */
  val stock = (quantity: Long, reservedCount: Long) => {
    quantity - reservedCount
  }

  /**
   * Prepare front end product url
   *
   * @return String
   */
  val url = (idCatalogConfig: Long, brandUrlKey: String, productName: String) => {
    ("%s-%s-d").format(brandUrlKey.replaceAll("/", ""), productName.replaceAll(" ", "-"), idCatalogConfig)
  }

  def simpleReservedDF(): DataFrame = {
    Model.salesOrderItem.filter("is_reserved = 1").groupBy("sku").agg(count("is_reserved") as "reservedCount").withColumnRenamed("sku", "simpleSku")
  }

  /**
   * Bob visibility data frame of simples
   * columns of data frame are "idCatalogSimple" & "visibility"
   *
   * @return DataFrame
   */
  def visibilityDF(): DataFrame = {
    val simpleDF = Model.simple.select(
      "sku",
      "fk_catalog_config",
      "id_catalog_simple"
    ).withColumnRenamed("sku", "simpleSku").
      withColumnRenamed("fk_catalog_config", "fkCatalogConfig").
      withColumnRenamed("id_catalog_simple", "idCatalogSimple")

    val image = Model.productImage.groupBy("fk_catalog_config").agg(count("image") as "imageCount")
    val configDF = Model.config.select(
      "id_catalog_config",
      "status",
      "status_supplier_config",
      "fk_catalog_supplier",
      "pet_approved",
      "fk_catalog_brand"
    ).withColumnRenamed("status", "configStatus").
      withColumnRenamed("id_catalog_config", "idCatalogConfig").
      withColumnRenamed("status_supplier_config", "statusSupplierConfig").
      withColumnRenamed("pet_approved", "petApproved")

    val supplierDF = configDF.
      join(
        Model.supplier.value.select("status", "id_catalog_supplier").
          withColumnRenamed("status", "supplierStatus"),
        configDF.col("fk_catalog_supplier") === Model.supplier.value.col("id_catalog_supplier")
      ).join(
          image,
          image.col("fk_catalog_config") === configDF.col("idCatalogConfig")
        ).join(
            Model.brand.value.select("status", "id_catalog_brand")
              .withColumnRenamed("status", "brandStatus"),
            Model.brand.value.col("id_catalog_brand") === configDF.col("fk_catalog_brand"),
            SQL.LEFT_OUTER
          ).select(
              "idCatalogConfig",
              "configStatus",
              "statusSupplierConfig",
              "supplierStatus",
              "brandStatus",
              "petApproved",
              "imageCount"
            )

    val sConfigDF = simpleDF.join(supplierDF, simpleDF.col("fkCatalogConfig") === supplierDF.col("idCatalogConfig"))

    val category = Model.category.value.select("status", "id_catalog_category").
      withColumnRenamed("status", "categoryStatus").
      withColumnRenamed("id_catalog_category", "idCatalogCategory")

    val categoryStatus = category.
      join(
        Model.categoryMapping.select("fk_catalog_category", "fk_catalog_config"),
        Model.categoryMapping.col("fk_catalog_category") === category.col("idCatalogCategory")
      ).select("categoryStatus", "fk_catalog_config").
        withColumnRenamed("fk_catalog_config", "fkCatalogConfig")

    val reservedDF = simpleReservedDF()
    val stockDataFrame = sConfigDF.
      join(
        categoryStatus,
        categoryStatus.col("fkCatalogConfig") === sConfigDF.col("idCatalogConfig"),
        SQL.LEFT_OUTER
      ).join(
          reservedDF,
          sConfigDF.col("simpleSku") === reservedDF.col("simpleSku"), SQL.LEFT_OUTER
        ).join(
            Model.catalogStock.select("fk_catalog_simple", "quantity"),
            Model.catalogStock.col("fk_catalog_simple") === sConfigDF.col("idCatalogSimple"),
            SQL.LEFT_OUTER
          )

    val visibilityUDF = udf(simpleVisibility)
    stockDataFrame.withColumn(
      "visibility",
      visibilityUDF(
        col("configStatus"),
        col("categoryStatus"),
        col("supplierStatus"),
        col("brandStatus"),
        col("statusSupplierConfig"),
        col("petApproved"),
        col("imageCount"),
        col("reservedCount"),
        col("quantity")
      )
    ).select(
        "idCatalogSimple",
        "visibility"
      ).
        withColumnRenamed(
          "id_catalog_simple",
          "idCatalogSimple"
        )

  }

  /**
   * Calculate visibility based upon the input parameter passed.
   *
   * status : status column of value of catalog_config of bob
   * categoryStatus : status column of catalog_category of bob
   * supplierStatus : status column of catalog_supplier of bob
   * brandStatus : status column of catalog_brand of bob
   * statusSupplierConfig : status_supplier_config of catalog_config of bob
   * petApproved : pet_approved column of catalog_config of bob
   * imageCount : total number of images again a product (count from catalog_product_image)
   * isReservedCount : total number of reserved count of sales_order_item
   * quantity : quantity column of catalog_stock of bob
   *
   */
  val simpleVisibility = (status: String, categoryStatus: String, supplierStatus: String, brandStatus: String, statusSupplierConfig: String, petApproved: Any, imageCount: Any, isReservedCount: Any, quantity: Any) => {
    var stock: Long = 0
    if (quantity != null && isReservedCount != null) {
      stock = quantity.toString.toLong - isReservedCount.toString.toLong
    }
    if (status == "active" &&
      categoryStatus == "active" &&
      supplierStatus == "active" &&
      brandStatus == "status" &&
      statusSupplierConfig == "active" &&
      petApproved.toString.toLong == 1 &&
      imageCount.toString.toLong > 0 &&
      stock > 0) {
      true
    } else {
      false
    }
  }
}
