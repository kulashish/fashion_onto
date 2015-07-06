package com.jabong.dap.model.product.itr

import com.jabong.dap.common.Spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.runtime.RichLong

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
      "fk_catalog_config"
    ).withColumnRenamed("id_catalog_simple", "idCatalogSimple").
      withColumnRenamed("special_price", "specialPrice").
      withColumnRenamed("special_to_date", "specialToDate").
      withColumnRenamed("special_from_date", "specialFromDate").
      withColumnRenamed("sku", "simpleSku")

    val visibilityDataFrame = visibilityDF()
    val vDF = visibilityDataFrame.select("idCatalogSimple", "visibility").withColumnRenamed("idCatalogSimple", "vIdCatalogSimple")
    val visibilityDaFr = simpleDF.join(
      vDF,
      vDF.col("vIdCatalogSimple") === simpleDF.col("idCatalogSimple"),
      "left_outer"
    )

    val stockDataFrame = stockDF()
    val sDF = stockDataFrame.select("idCatalogSimple", "quantity").withColumnRenamed("idCatalogSimple", "stockIdCatalogSimple")
    val quantityDF = visibilityDaFr.join(
      sDF,
      sDF.col("stockIdCatalogSimple") === simpleDF.col("idCatalogSimple"),
      "left_outer"
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
      withColumnRenamed("id_catalog_config", "idCatalogConfig").
      withColumnRenamed("name", "productName").
      withColumnRenamed("special_margin", "specialMargin").
      withColumnRenamed("activated_at", "activatedAt").
      withColumnRenamed("sku", "configSku")

    val configDF = quantityDF.join(
      config,
      config("idCatalogConfig") === simpleDF("fk_catalog_config")
    )

    val supplierDF = configDF.join(
      Model.supplier.value.select("status", "id_catalog_supplier").withColumnRenamed("status", "supplierStatus"),
      Model.config("fk_catalog_supplier") === Model.supplier.value("id_catalog_supplier")
    )

    val productUrl = udf(url)

    val brandDF = supplierDF.join(
      Model.brand.value.select("url_key", "id_catalog_brand").withColumnRenamed("url_key", "brandUrlKey"),
      configDF("fk_catalog_brand") === Model.brand.value("id_catalog_brand")
    ).withColumn(
        "productUrl",
        productUrl(col("idCatalogConfig"), col("brandUrlKey"), col("productName"))
      )

    brandDF.
      select(
        "idCatalogSimple",
        "specialToDate",
        "specialFromDate",
        "simpleSku",
        "visibility",
        "quantity",
        "idCatalogConfig",
        "productName",
        "activatedAt",
        "configSku",
        "supplierStatus",
        "productUrl",
        "specialMargin",
        "margin",
        "specialPrice"
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
        "left_outer"
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
   * @return Int
   */
  val stock = (quantity: Int, reservedCount: Int) => {
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
            "left_outer"
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
        "left_outer"
      ).join(
          reservedDF,
          sConfigDF.col("simpleSku") === reservedDF.col("simpleSku"), "left_outer"
        ).join(
            Model.catalogStock.select("fk_catalog_simple", "quantity"),
            Model.catalogStock.col("fk_catalog_simple") === sConfigDF.col("idCatalogSimple"),
            "left_outer"
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
